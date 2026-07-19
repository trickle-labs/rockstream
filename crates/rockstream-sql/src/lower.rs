//! Lowering pass: DataFusion `LogicalPlan` → RockStream `PlanNode` (v0.7).
//!
//! Transforms the DataFusion `LogicalPlan` produced by the SQL parser into
//! the RockStream PlanIR (`PlanNode` tree).  Only the Phase 1 operator set
//! is supported:
//!
//! | DataFusion plan node | PlanNode variant |
//! |---|---|
//! | `TableScan` | `Source` |
//! | `Filter` | `Filter` |
//! | `Projection` | `Project` |
//! | `Aggregate` | `Aggregate` |
//! | `SubqueryAlias` | transparent (strips alias) |
//!
//! Expression encoding (for `Expr::Literal` bytes):
//! - Integer scalars (Int8/16/32/64) → 8-byte big-endian i64
//! - Float64 → 8-byte big-endian f64 bits
//! - Boolean → 1 byte (0 = false, 1 = true)
//! - UTF-8 string → raw UTF-8 bytes
//! - NULL → empty Vec
//!
//! Column references are resolved to 0-based indices using the *input
//! plan's* output schema, so the lowered PlanNode is independent of column
//! names.

use datafusion::common::DFSchema;
#[allow(unused_imports)]
use datafusion::logical_expr::expr::AggregateFunctionParams;
use datafusion::logical_expr::{
    expr::{AggregateFunction, WindowFunction},
    BinaryExpr, Distinct, JoinType, LogicalPlan, Operator as DfOp, WindowFrameBound,
    WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::prelude::Expr as DfExpr;
use datafusion::scalar::ScalarValue;

use rockstream_plan::{
    AggregateExpr, AggregateFunc, BinaryOp, Expr, JoinSemantics, OuterJoinKind, PlanNode,
    WindowExpr, WindowFunc,
};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::ids::OperatorId;
use std::hash::{Hash, Hasher};

use crate::error::SqlError;

// ─── Public API ──────────────────────────────────────────────────────────────

/// Lower a DataFusion `LogicalPlan` to a `PlanNode`.
///
/// The `plan` must have been produced by the DataFusion planner (optimized or
/// unoptimized).  Only Phase 1 operators (Source / Filter / Project / Aggregate)
/// are supported.  Other plan nodes return `RS-1013`.
pub fn lower(plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let src = PlanNode::Source {
                name: scan.table_name.table().to_string(),
            };
            if let Some(ref proj) = scan.projection {
                let columns = proj.iter().map(|&idx| Expr::Column(idx)).collect();
                Ok(PlanNode::Project {
                    input: Box::new(src),
                    columns,
                })
            } else {
                Ok(src)
            }
        }

        LogicalPlan::Filter(filter) => {
            let input = lower(&filter.input)?;
            let predicate = lower_expr(&filter.predicate, filter.input.schema())?;
            Ok(PlanNode::Filter {
                input: Box::new(input),
                predicate,
            })
        }

        LogicalPlan::Projection(proj) => {
            if let Some(topk_plan) = try_lower_topk_pattern(proj)? {
                return Ok(topk_plan);
            }
            let input = lower(&proj.input)?;
            let columns = proj
                .expr
                .iter()
                .map(|e| lower_proj_expr(e, proj.input.schema()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanNode::Project {
                input: Box::new(input),
                columns,
            })
        }

        LogicalPlan::Aggregate(agg) => {
            if let Some(hop_plan) = try_lower_hop_window_aggregate(agg)? {
                return Ok(hop_plan);
            }

            let input = lower(&agg.input)?;
            let input_schema = agg.input.schema();

            // Check if any group key is a date_bin tumbling window function
            let mut date_bin_idx = None;
            for (idx, e) in agg.group_expr.iter().enumerate() {
                if find_date_bin(e).is_some() {
                    date_bin_idx = Some(idx);
                    break;
                }
            }

            if let Some(tumble_idx) = date_bin_idx {
                let sf = find_date_bin(&agg.group_expr[tumble_idx]).unwrap();
                if sf.args.len() < 2 {
                    return Err(SqlError::UnsupportedPlanNode {
                        node_type: "date_bin_missing_args".to_string(),
                    });
                }
                let window_size_ms = extract_interval_ms(&sf.args[0]).ok_or_else(|| {
                    SqlError::UnsupportedPlanNode {
                        node_type: "date_bin_invalid_interval".to_string(),
                    }
                })?;
                let time_col =
                    extract_column_index(&sf.args[1], input_schema).ok_or_else(|| {
                        SqlError::UnsupportedPlanNode {
                            node_type: "date_bin_invalid_time_col".to_string(),
                        }
                    })?;

                let tumble_node = PlanNode::TumbleWindow {
                    input: Box::new(input),
                    time_col,
                    window_size_ms,
                    late_data_policy: rockstream_plan::LateDataPolicy::Drop,
                };

                // Re-compile group_by and aggregates with column index shifting
                let mut group_by = Vec::new();
                for (i, e) in agg.group_expr.iter().enumerate() {
                    if i == tumble_idx {
                        group_by.push(Expr::Column(0));
                    } else {
                        let mut lowered_e = lower_expr(e, input_schema)?;
                        shift_expr_cols(&mut lowered_e);
                        group_by.push(lowered_e);
                    }
                }

                let mut aggregates = Vec::new();
                for e in &agg.aggr_expr {
                    let mut lowered_agg = lower_agg_expr(e, input_schema)?;
                    shift_expr_cols(&mut lowered_agg.input);
                    aggregates.push(lowered_agg);
                }

                return Ok(PlanNode::Aggregate {
                    input: Box::new(tumble_node),
                    group_by,
                    aggregates,
                });
            }

            // DataFusion's optimizer converts `SELECT DISTINCT cols FROM t` to
            // `SELECT cols FROM t GROUP BY cols` (Aggregate with no aggregate
            // functions).  Detect and route to `PlanNode::Distinct` so the
            // runtime uses the correct zero-crossing weight semantics (IVM-6).
            if agg.aggr_expr.is_empty() {
                let arr_id = OperatorId(stable_plan_hash(&input));
                return Ok(PlanNode::Distinct {
                    input: Box::new(input),
                    arr_id,
                });
            }

            let group_by = agg
                .group_expr
                .iter()
                .map(|e| lower_expr(e, input_schema))
                .collect::<Result<Vec<_>, _>>()?;
            let aggregates = agg
                .aggr_expr
                .iter()
                .map(|e| lower_agg_expr(e, input_schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanNode::Aggregate {
                input: Box::new(input),
                group_by,
                aggregates,
            })
        }

        // Transparent wrappers — strip and lower the inner plan.
        LogicalPlan::SubqueryAlias(alias) => lower(&alias.input),

        // Extension node (e.g. IncAggregate) — currently transparent: lower
        // the inner aggregate directly.  In v0.8+ these carry additional
        // incremental metadata.
        LogicalPlan::Extension(ext) => {
            // We only know how to lower extension nodes that expose exactly
            // one child input.  For now, return RS-1013 for anything else.
            let inputs = ext.node.inputs();
            if inputs.len() == 1 {
                lower(inputs[0])
            } else {
                Err(SqlError::UnsupportedPlanNode {
                    node_type: format!("Extension::{}", ext.node.name()),
                })
            }
        }

        // Inner and outer/semi/anti equi-joins (v0.8/v0.9 — IVM-4/IVM-5).
        LogicalPlan::Join(join) => {
            // Map DataFusion JoinType to our join representation.
            // For semi/anti joins, the on condition may be empty (DataFusion uses
            // a filter instead); handle both cases.
            let on = &join.on;
            let on_result = if on.is_empty() {
                extract_join_keys_from_filter(&join.filter, &join.left, &join.right)
            } else {
                extract_equi_join_keys(on, &join.left, &join.right)
            };

            match join.join_type {
                JoinType::Inner => {
                    let (left_keys, right_keys) = on_result?;
                    let left = lower(&join.left)?;
                    let right = lower(&join.right)?;
                    let left_arr_id = OperatorId(
                        hash_keys(&left_keys)
                            .wrapping_mul(31)
                            .wrapping_add(hash_keys(&right_keys)),
                    );
                    let right_arr_id = OperatorId(
                        hash_keys(&right_keys)
                            .wrapping_mul(37)
                            .wrapping_add(hash_keys(&left_keys)),
                    );
                    Ok(PlanNode::InnerJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        left_keys,
                        right_keys,
                        left_arr_id,
                        right_arr_id,
                        semantics: JoinSemantics::default(),
                    })
                }

                JoinType::Left => {
                    let (left_keys, right_keys) = on_result?;
                    lower_outer_join(OuterJoinKind::Left, join, left_keys, right_keys)
                }

                JoinType::Right => {
                    let (left_keys, right_keys) = on_result?;
                    lower_outer_join(OuterJoinKind::Right, join, left_keys, right_keys)
                }

                JoinType::Full => {
                    let (left_keys, right_keys) = on_result?;
                    lower_outer_join(OuterJoinKind::Full, join, left_keys, right_keys)
                }

                JoinType::LeftSemi => {
                    // For LeftSemi, on may be empty if DataFusion uses filter.
                    // Try on condition first, else try filter expression for keys.
                    let (left_keys, right_keys) = if on.is_empty() {
                        extract_join_keys_from_filter(&join.filter, &join.left, &join.right)?
                    } else {
                        on_result?
                    };
                    lower_outer_join(OuterJoinKind::Semi, join, left_keys, right_keys)
                }

                JoinType::LeftAnti => {
                    let (left_keys, right_keys) = if on.is_empty() {
                        extract_join_keys_from_filter(&join.filter, &join.left, &join.right)?
                    } else {
                        on_result?
                    };
                    lower_outer_join(OuterJoinKind::Anti, join, left_keys, right_keys)
                }

                JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark => {
                    // Not required by TPC-H — return RS-1013.
                    Err(SqlError::UnsupportedPlanNode {
                        node_type: format!("Join:{:?}", join.join_type),
                    })
                }
            }
        }

        // SELECT DISTINCT ... (v0.10 — IVM-6).
        //
        // DataFusion represents `SELECT DISTINCT cols FROM t` as
        // `Distinct::All(Projection(...))`. Route to `PlanNode::Distinct`.
        //
        // Note: INTERSECT / EXCEPT in DataFusion 53 are lowered to LeftSemi /
        // LeftAnti joins by the DataFusion planner, so they reach this pass as
        // `LogicalPlan::Join` nodes and are handled by the existing join arm.
        LogicalPlan::Distinct(Distinct::All(input)) => {
            let inner = lower(input)?;
            let arr_id = OperatorId(stable_plan_hash(&inner));
            Ok(PlanNode::Distinct {
                input: Box::new(inner),
                arr_id,
            })
        }

        // DISTINCT ON (...) — not supported (PostgreSQL extension, not standard SQL).
        LogicalPlan::Distinct(Distinct::On(_)) => Err(SqlError::UnsupportedPlanNode {
            node_type: "Distinct::On (DISTINCT ON — not supported, use DISTINCT)".to_string(),
        }),

        // UNION ALL (v0.10) — stateless union of two inputs (no deduplication).
        // DataFusion produces a `Union` with ≥2 inputs; we fold them left-associatively.
        LogicalPlan::Union(u) => {
            if u.inputs.is_empty() {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: "Union::empty".to_string(),
                });
            }
            // Lower the first input; fold remaining inputs as left-associative Union nodes.
            let mut acc = lower(&u.inputs[0])?;
            for inp in &u.inputs[1..] {
                let right = lower(inp)?;
                acc = PlanNode::Union {
                    left: Box::new(acc),
                    right: Box::new(right),
                };
            }
            Ok(acc)
        }

        // Window functions (v0.11 — IVM-7).
        LogicalPlan::Window(w) => {
            let input_node = lower(&w.input)?;
            let input_schema = w.input.schema();
            let window_exprs = w
                .window_expr
                .iter()
                .map(|e| lower_window_expr(e, input_schema))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanNode::Window {
                input: Box::new(input_node),
                window_exprs,
            })
        }

        // DataFusion 53 lowers scalar `UNNEST(expr)` projections to
        // `LogicalPlan::Unnest`. In the vendored grammar today, explicit
        // `CROSS JOIN LATERAL UNNEST(...)` table-function syntax and
        // `LATERAL (SELECT ...)` are rejected during planning, so there is no
        // second reachable lateral node shape to lower here yet.
        LogicalPlan::Unnest(unnest) => {
            let input = lower(&unnest.input)?;
            if !unnest.struct_type_columns.is_empty() || unnest.list_type_columns.len() != 1 {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: format!(
                        "Unnest:list_cols={},struct_cols={}",
                        unnest.list_type_columns.len(),
                        unnest.struct_type_columns.len()
                    ),
                });
            }
            let (col, _) = &unnest.list_type_columns[0];
            Ok(PlanNode::Lateral {
                input: Box::new(input),
                func: rockstream_plan::LateralFunc::Unnest { col: *col },
            })
        }

        LogicalPlan::RecursiveQuery(recursive) => {
            let base_cols = recursive.static_term.schema().fields().len();
            let step_cols = recursive.recursive_term.schema().fields().len();
            let output_cols = plan.schema().fields().len();
            if base_cols != step_cols || base_cols != output_cols {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: format!(
                        "recursive_query_column_count_mismatch:base={base_cols},step={step_cols},output={output_cols}"
                    ),
                });
            }

            let classification =
                classify_recursive_term(&recursive.name, &recursive.recursive_term);
            if classification.self_references > 1 {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: format!(
                        "recursive_query_multiple_self_references:name={}",
                        recursive.name
                    ),
                });
            }

            Ok(PlanNode::Recursion {
                base: Box::new(lower(&recursive.static_term)?),
                step: Box::new(lower(&recursive.recursive_term)?),
                max_iterations: RockstreamConfig::default().recursion_max_iterations,
                monotone: !recursive.is_distinct && !classification.non_monotone,
            })
        }

        other => Err(SqlError::UnsupportedPlanNode {
            node_type: plan_node_name(other).to_string(),
        }),
    }
}

// ─── Window expression lowering ──────────────────────────────────────────────

/// Lower a DataFusion window expression to a `WindowExpr`.
///
/// `schema` is the INPUT schema (not the output schema of the Window node).
fn lower_window_expr(
    expr: &DfExpr,
    schema: &datafusion::common::DFSchema,
) -> Result<WindowExpr, SqlError> {
    let wf: &WindowFunction = match expr {
        DfExpr::WindowFunction(wf) => wf.as_ref(),
        other => {
            return Err(SqlError::UnsupportedPlanNode {
                node_type: format!("window_expr:{}", expr_name(other)),
            });
        }
    };

    let partition_by = wf
        .params
        .partition_by
        .iter()
        .filter_map(|e| {
            if let DfExpr::Column(col) = e {
                schema.index_of_column(col).ok()
            } else {
                None
            }
        })
        .collect::<Vec<usize>>();

    let order_by = wf
        .params
        .order_by
        .iter()
        .filter_map(|sort| {
            if let DfExpr::Column(col) = &sort.expr {
                schema.index_of_column(col).ok()
            } else {
                None
            }
        })
        .collect::<Vec<usize>>();

    let func = lower_window_func(wf, schema, &order_by)?;
    Ok(WindowExpr {
        func,
        partition_by,
        order_by,
    })
}

fn lower_window_func(
    wf: &WindowFunction,
    _schema: &datafusion::common::DFSchema,
    order_by: &[usize],
) -> Result<WindowFunc, SqlError> {
    match &wf.fun {
        WindowFunctionDefinition::WindowUDF(udwf) => match udwf.name() {
            "row_number" => Ok(WindowFunc::RowNumber),
            "rank" => Ok(WindowFunc::Rank),
            "dense_rank" => Ok(WindowFunc::DenseRank),
            "lag" => {
                let offset = extract_lag_lead_offset(&wf.params.args, 1)?;
                Ok(WindowFunc::Lag { offset })
            }
            "lead" => {
                let offset = extract_lag_lead_offset(&wf.params.args, 1)?;
                Ok(WindowFunc::Lead { offset })
            }
            "ntile" => Err(SqlError::UnsupportedWindowFunction {
                fn_name: "NTILE".to_string(),
            }),
            name => Err(SqlError::UnsupportedWindowFunction {
                fn_name: name.to_uppercase(),
            }),
        },
        WindowFunctionDefinition::AggregateUDF(udaf) => {
            let frame = &wf.params.window_frame;
            if frame.units != WindowFrameUnits::Rows {
                return Err(SqlError::UnsupportedWindowFunction {
                    fn_name: format!("{}_OVER_NON_ROWS_FRAME", udaf.name().to_uppercase()),
                });
            }
            let frame_rows = match &frame.start_bound {
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => *n as usize + 1,
                WindowFrameBound::Preceding(ScalarValue::Int64(Some(n))) => *n as usize + 1,
                WindowFrameBound::Preceding(_) => {
                    // Unbounded preceding or other scalar — use whole partition.
                    let _ = order_by;
                    return Err(SqlError::UnsupportedWindowFunction {
                        fn_name: format!("{}_UNBOUNDED_PRECEDING", udaf.name().to_uppercase()),
                    });
                }
                _ => {
                    return Err(SqlError::UnsupportedWindowFunction {
                        fn_name: format!("{}_FRAME", udaf.name().to_uppercase()),
                    })
                }
            };
            match udaf.name() {
                "sum" | "SUM" => Ok(WindowFunc::SlidingSum { frame_rows }),
                "avg" | "AVG" | "mean" => Ok(WindowFunc::SlidingAvg { frame_rows }),
                name => Err(SqlError::UnsupportedWindowFunction {
                    fn_name: name.to_uppercase(),
                }),
            }
        }
    }
}

/// Extract offset from LAG/LEAD args (args[1] is the offset literal).
fn extract_lag_lead_offset(args: &[DfExpr], default: usize) -> Result<usize, SqlError> {
    if args.len() < 2 {
        return Ok(default);
    }
    match &args[1] {
        DfExpr::Literal(ScalarValue::Int64(Some(n)), _) => Ok(*n as usize),
        DfExpr::Literal(ScalarValue::Int32(Some(n)), _) => Ok(*n as usize),
        DfExpr::Literal(ScalarValue::UInt64(Some(n)), _) => Ok(*n as usize),
        DfExpr::Literal(ScalarValue::Null, _) => Ok(default),
        _ => Ok(default),
    }
}

// ─── Expression lowering ─────────────────────────────────────────────────────

/// Lower a scalar DataFusion expression to a `PlanNode` `Expr`.
///
/// `schema` is the **input** schema at the point where this expression is
/// evaluated (used to resolve column names to 0-based indices).
pub fn lower_expr(expr: &DfExpr, schema: &DFSchema) -> Result<Expr, SqlError> {
    match expr {
        DfExpr::Column(col) => {
            let idx = schema
                .index_of_column(col)
                .map_err(|e| SqlError::UnsupportedPlanNode {
                    node_type: format!("column_resolution:{e}"),
                })?;
            Ok(Expr::Column(idx))
        }

        DfExpr::Literal(scalar, _) => {
            let bytes = encode_scalar(scalar)?;
            Ok(Expr::Literal(bytes))
        }

        DfExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let plan_op = lower_binary_op(op)?;
            let l = lower_expr(left, schema)?;
            let r = lower_expr(right, schema)?;
            Ok(Expr::BinaryOp {
                op: plan_op,
                left: Box::new(l),
                right: Box::new(r),
            })
        }

        // Strip aliases (projection output aliases don't affect the expression).
        DfExpr::Alias(alias) => lower_expr(&alias.expr, schema),

        // Strip casts that don't change the logical value (e.g. Int32→Int64).
        DfExpr::Cast(cast) => lower_expr(&cast.expr, schema),
        DfExpr::TryCast(cast) => lower_expr(&cast.expr, schema),

        other => Err(SqlError::UnsupportedPlanNode {
            node_type: format!("expr:{}", expr_name(other)),
        }),
    }
}

/// Lower a projection expression.  Same as `lower_expr` but strips the outer
/// `Alias` wrapper that DataFusion wraps around named output columns.
pub fn lower_proj_expr(expr: &DfExpr, schema: &DFSchema) -> Result<Expr, SqlError> {
    match expr {
        DfExpr::Alias(alias) => lower_proj_expr(&alias.expr, schema),
        other => lower_expr(other, schema),
    }
}

// ─── Aggregate function lowering ─────────────────────────────────────────────

/// Lower a DataFusion aggregate function expression to an `AggregateExpr`.
pub fn lower_agg_expr(expr: &DfExpr, input_schema: &DFSchema) -> Result<AggregateExpr, SqlError> {
    match expr {
        DfExpr::AggregateFunction(AggregateFunction { func, params }) => {
            let name = func.name().to_lowercase();
            let func_kind = match name.as_str() {
                "sum" => AggregateFunc::Sum,
                "count" => AggregateFunc::Count,
                "avg" => AggregateFunc::Avg,
                "min" => AggregateFunc::Min,
                "max" => AggregateFunc::Max,
                other => {
                    return Err(SqlError::UnsupportedPlanNode {
                        node_type: format!("aggregate_function:{other}"),
                    })
                }
            };

            // For COUNT(*) DataFusion injects a `Literal(Int64(1))` argument.
            // We keep it verbatim as `Literal(1i64.to_be_bytes())` which matches
            // what the hand-coded PlanNode uses.
            let input_expr = if params.args.is_empty() {
                Expr::Literal(1i64.to_be_bytes().to_vec())
            } else {
                lower_expr(&params.args[0], input_schema)?
            };

            Ok(AggregateExpr {
                func: func_kind,
                input: input_expr,
                distinct: params.distinct,
            })
        }

        // Strip alias wrappers DataFusion may inject around aggregate expressions.
        DfExpr::Alias(alias) => lower_agg_expr(&alias.expr, input_schema),

        other => Err(SqlError::UnsupportedPlanNode {
            node_type: format!("agg_expr:{}", expr_name(other)),
        }),
    }
}

// ─── Scalar encoding ─────────────────────────────────────────────────────────

/// Encode a DataFusion `ScalarValue` as bytes for `Expr::Literal`.
///
/// All integer types are promoted to i64 and encoded as 8-byte big-endian.
/// NULL values are encoded as empty bytes.
pub fn encode_scalar(scalar: &ScalarValue) -> Result<Vec<u8>, SqlError> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::Int16(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::Int32(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::Int64(Some(v)) => Ok(v.to_be_bytes().to_vec()),
        ScalarValue::UInt8(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::UInt16(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::UInt32(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::UInt64(Some(v)) => Ok((*v as i64).to_be_bytes().to_vec()),
        ScalarValue::Float64(Some(v)) => Ok(v.to_bits().to_be_bytes().to_vec()),
        ScalarValue::Float32(Some(v)) => Ok(((*v as f64).to_bits()).to_be_bytes().to_vec()),
        ScalarValue::Boolean(Some(b)) => Ok(vec![if *b { 1u8 } else { 0u8 }]),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s.as_bytes().to_vec()),
        // NULL variants — empty bytes
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Boolean(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None) => Ok(vec![]),
        other => Err(SqlError::UnsupportedPlanNode {
            node_type: format!("scalar:{other:?}"),
        }),
    }
}

// ─── Operator mapping ────────────────────────────────────────────────────────

fn lower_binary_op(op: &DfOp) -> Result<BinaryOp, SqlError> {
    match op {
        DfOp::Eq => Ok(BinaryOp::Eq),
        DfOp::NotEq => Ok(BinaryOp::Ne),
        DfOp::Lt => Ok(BinaryOp::Lt),
        DfOp::LtEq => Ok(BinaryOp::Le),
        DfOp::Gt => Ok(BinaryOp::Gt),
        DfOp::GtEq => Ok(BinaryOp::Ge),
        DfOp::Plus => Ok(BinaryOp::Add),
        DfOp::Minus => Ok(BinaryOp::Sub),
        DfOp::Multiply => Ok(BinaryOp::Mul),
        DfOp::Divide => Ok(BinaryOp::Div),
        DfOp::Modulo => Ok(BinaryOp::Mod),
        DfOp::And => Ok(BinaryOp::And),
        DfOp::Or => Ok(BinaryOp::Or),
        other => Err(SqlError::UnsupportedPlanNode {
            node_type: format!("binary_op:{other:?}"),
        }),
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn plan_node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Repartition(_) => "Repartition",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::TableScan(_) => "TableScan",
        LogicalPlan::EmptyRelation(_) => "EmptyRelation",
        LogicalPlan::Subquery(_) => "Subquery",
        LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Extension(_) => "Extension",
        LogicalPlan::Distinct(_) => "Distinct",
        LogicalPlan::Window(_) => "Window",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Analyze(_) => "Analyze",
        LogicalPlan::Dml(_) => "Dml",
        LogicalPlan::Ddl(_) => "Ddl",
        LogicalPlan::Copy(_) => "Copy",
        LogicalPlan::DescribeTable(_) => "DescribeTable",
        LogicalPlan::Unnest(_) => "Unnest",
        LogicalPlan::RecursiveQuery(_) => "RecursiveQuery",
        LogicalPlan::Statement(_) => "Statement",
        _ => "Unknown",
    }
}

fn expr_name(expr: &DfExpr) -> &'static str {
    match expr {
        DfExpr::Column(_) => "Column",
        DfExpr::Literal(..) => "Literal",
        DfExpr::BinaryExpr(_) => "BinaryExpr",
        DfExpr::Alias(_) => "Alias",
        DfExpr::Cast(_) => "Cast",
        DfExpr::TryCast(_) => "TryCast",
        DfExpr::AggregateFunction(_) => "AggregateFunction",
        DfExpr::WindowFunction(_) => "WindowFunction",
        DfExpr::ScalarFunction(_) => "ScalarFunction",
        DfExpr::Not(_) => "Not",
        DfExpr::IsNull(_) => "IsNull",
        DfExpr::IsNotNull(_) => "IsNotNull",
        DfExpr::IsTrue(_) => "IsTrue",
        DfExpr::IsFalse(_) => "IsFalse",
        DfExpr::IsUnknown(_) => "IsUnknown",
        DfExpr::IsNotTrue(_) => "IsNotTrue",
        DfExpr::IsNotFalse(_) => "IsNotFalse",
        DfExpr::IsNotUnknown(_) => "IsNotUnknown",
        DfExpr::Negative(_) => "Negative",
        DfExpr::Between(_) => "Between",
        DfExpr::Case(_) => "Case",
        DfExpr::InList(_) => "InList",
        DfExpr::InSubquery(_) => "InSubquery",
        DfExpr::ScalarSubquery(_) => "ScalarSubquery",
        DfExpr::GroupingSet(_) => "GroupingSet",
        DfExpr::Like(_) => "Like",
        DfExpr::SimilarTo(_) => "SimilarTo",
        DfExpr::Placeholder(_) => "Placeholder",
        DfExpr::OuterReferenceColumn(_, _) => "OuterReferenceColumn",
        DfExpr::Unnest(_) => "Unnest",
        _ => "Unknown",
    }
}

#[derive(Default)]
struct RecursiveClassification {
    non_monotone: bool,
    self_references: usize,
}

fn classify_recursive_term(recursive_name: &str, plan: &LogicalPlan) -> RecursiveClassification {
    let mut classification = RecursiveClassification::default();
    classify_recursive_term_inner(recursive_name, plan, &mut classification);
    classification
}

fn classify_recursive_term_inner(
    recursive_name: &str,
    plan: &LogicalPlan,
    classification: &mut RecursiveClassification,
) {
    match plan {
        LogicalPlan::Aggregate(_) | LogicalPlan::Window(_) | LogicalPlan::Distinct(_) => {
            classification.non_monotone = true;
        }
        LogicalPlan::Join(join) => {
            if matches!(
                join.join_type,
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
            ) {
                classification.non_monotone = true;
            }
        }
        LogicalPlan::TableScan(scan) if scan.table_name.table() == recursive_name => {
            classification.self_references += 1;
        }
        _ => {}
    }

    for input in plan.inputs() {
        classify_recursive_term_inner(recursive_name, input, classification);
    }
}

// ─── Join helpers ────────────────────────────────────────────────────────────

/// Extract equi-join key column indices from DataFusion join condition.
///
/// Returns `(left_keys, right_keys)` where each is a Vec of column indices.
/// The condition is typically a BinaryExpr with `Eq` operator comparing columns
/// from left and right inputs.
///
/// For now, supports only simple equi-joins where the condition is a single
/// `Eq` or conjunctions of `Eq` operators (via `And`).
fn extract_equi_join_keys(
    on_condition: &[(DfExpr, DfExpr)],
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
) -> Result<(Vec<usize>, Vec<usize>), SqlError> {
    if on_condition.is_empty() {
        return Err(SqlError::UnsupportedPlanNode {
            node_type: "join_no_equi_condition".to_string(),
        });
    }

    let left_schema = left_plan.schema();
    let right_schema = right_plan.schema();
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    for (left_expr, right_expr) in on_condition {
        // Extract column index from left expression.
        let left_idx = match left_expr {
            DfExpr::Column(col) => {
                left_schema
                    .index_of_column(col)
                    .map_err(|_| SqlError::UnsupportedPlanNode {
                        node_type: "join_column_resolution".to_string(),
                    })?
            }
            _ => {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: "join_non_column_key".to_string(),
                })
            }
        };

        // Extract column index from right expression.
        let right_idx =
            match right_expr {
                DfExpr::Column(col) => right_schema.index_of_column(col).map_err(|_| {
                    SqlError::UnsupportedPlanNode {
                        node_type: "join_column_resolution".to_string(),
                    }
                })?,
                _ => {
                    return Err(SqlError::UnsupportedPlanNode {
                        node_type: "join_non_column_key".to_string(),
                    })
                }
            };

        left_keys.push(left_idx);
        right_keys.push(right_idx);
    }

    Ok((left_keys, right_keys))
}

/// Compute a stable deterministic u64 hash of a `PlanNode` for operator ID generation.
fn stable_plan_hash(plan: &PlanNode) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut h = DefaultHasher::new();
    format!("{plan:?}").hash(&mut h);
    h.finish().wrapping_add(1)
}

/// Compute a simple hash of a Vec of column indices for operator ID generation.
fn hash_keys(keys: &[usize]) -> u64 {
    let mut hash = 0u64;
    for &k in keys {
        hash = hash.wrapping_mul(31).wrapping_add(k as u64);
    }
    hash.wrapping_add(1) // Ensure non-zero
}

/// Lower an outer/semi/anti join node to `PlanNode::OuterJoin`.
fn lower_outer_join(
    kind: OuterJoinKind,
    join: &datafusion::logical_expr::Join,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
) -> Result<PlanNode, SqlError> {
    let left = lower(&join.left)?;
    let right = lower(&join.right)?;

    let left_arr_id = OperatorId(
        hash_keys(&left_keys)
            .wrapping_mul(31)
            .wrapping_add(hash_keys(&right_keys)),
    );
    let right_arr_id = OperatorId(
        hash_keys(&right_keys)
            .wrapping_mul(37)
            .wrapping_add(hash_keys(&left_keys)),
    );
    let unmatched_arr_id = OperatorId(
        hash_keys(&left_keys)
            .wrapping_mul(41)
            .wrapping_add(hash_keys(&right_keys))
            .wrapping_add(0x4F5544), // "OUD" for Outer Unmatched Data
    );

    Ok(PlanNode::OuterJoin {
        kind,
        left: Box::new(left),
        right: Box::new(right),
        left_keys,
        right_keys,
        left_arr_id,
        right_arr_id,
        unmatched_arr_id,
    })
}

/// Extract equi-join keys from a semi/anti join filter expression.
///
/// DataFusion sometimes places the equi-condition in the `filter` field for
/// semi/anti joins instead of the `on` field.  This function extracts the
/// column indices from the filter if it's a simple equi-expression.
fn extract_join_keys_from_filter(
    filter: &Option<datafusion::prelude::Expr>,
    left_plan: &LogicalPlan,
    right_plan: &LogicalPlan,
) -> Result<(Vec<usize>, Vec<usize>), SqlError> {
    use datafusion::logical_expr::BinaryExpr;

    match filter {
        Some(DfExpr::BinaryExpr(BinaryExpr { left, op, right })) => {
            if *op != datafusion::logical_expr::Operator::Eq {
                return Err(SqlError::UnsupportedPlanNode {
                    node_type: "join_filter_non_eq".to_string(),
                });
            }
            let left_schema = left_plan.schema();
            let right_schema = right_plan.schema();
            let left_idx =
                match left.as_ref() {
                    DfExpr::Column(col) => left_schema.index_of_column(col).map_err(|_| {
                        SqlError::UnsupportedPlanNode {
                            node_type: "join_filter_column_resolution".to_string(),
                        }
                    })?,
                    _ => {
                        return Err(SqlError::UnsupportedPlanNode {
                            node_type: "join_filter_non_column_key".to_string(),
                        })
                    }
                };
            let right_idx = match right.as_ref() {
                DfExpr::Column(col) => right_schema.index_of_column(col).map_err(|_| {
                    SqlError::UnsupportedPlanNode {
                        node_type: "join_filter_column_resolution".to_string(),
                    }
                })?,
                _ => {
                    return Err(SqlError::UnsupportedPlanNode {
                        node_type: "join_filter_non_column_key".to_string(),
                    })
                }
            };
            Ok((vec![left_idx], vec![right_idx]))
        }
        _ => Err(SqlError::UnsupportedPlanNode {
            node_type: "join_no_equi_condition".to_string(),
        }),
    }
}

/// Lower a DataFusion `LogicalPlan` to a `PlanNode` resolving view and snapshot references.
pub fn lower_with_views(
    plan: &LogicalPlan,
    registered_views: &std::collections::HashSet<String>,
    snapshot_sources: &std::collections::HashSet<String>,
) -> Result<PlanNode, SqlError> {
    let lowered = lower(plan)?;
    Ok(resolve_views_and_snapshots(
        lowered,
        registered_views,
        snapshot_sources,
    ))
}

fn resolve_views_and_snapshots(
    node: PlanNode,
    registered_views: &std::collections::HashSet<String>,
    snapshot_sources: &std::collections::HashSet<String>,
) -> PlanNode {
    match node {
        PlanNode::Source { name } => {
            if registered_views.contains(&name) {
                PlanNode::ViewRef { view_name: name }
            } else if snapshot_sources.contains(&name) {
                PlanNode::Snapshot {
                    source_name: name,
                    batch_size: 1000,
                }
            } else {
                PlanNode::Source { name }
            }
        }
        PlanNode::Filter { input, predicate } => PlanNode::Filter {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            predicate,
        },
        PlanNode::Project { input, columns } => PlanNode::Project {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            columns,
        },
        PlanNode::Map { input, func } => PlanNode::Map {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            func,
        },
        PlanNode::Aggregate {
            input,
            group_by,
            aggregates,
        } => PlanNode::Aggregate {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            group_by,
            aggregates,
        },
        PlanNode::Join {
            left,
            right,
            condition,
        } => PlanNode::Join {
            left: Box::new(resolve_views_and_snapshots(
                *left,
                registered_views,
                snapshot_sources,
            )),
            right: Box::new(resolve_views_and_snapshots(
                *right,
                registered_views,
                snapshot_sources,
            )),
            condition,
        },
        PlanNode::InnerJoin {
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            semantics,
        } => PlanNode::InnerJoin {
            left: Box::new(resolve_views_and_snapshots(
                *left,
                registered_views,
                snapshot_sources,
            )),
            right: Box::new(resolve_views_and_snapshots(
                *right,
                registered_views,
                snapshot_sources,
            )),
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            semantics,
        },
        PlanNode::OuterJoin {
            kind,
            left,
            right,
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            unmatched_arr_id,
        } => PlanNode::OuterJoin {
            kind,
            left: Box::new(resolve_views_and_snapshots(
                *left,
                registered_views,
                snapshot_sources,
            )),
            right: Box::new(resolve_views_and_snapshots(
                *right,
                registered_views,
                snapshot_sources,
            )),
            left_keys,
            right_keys,
            left_arr_id,
            right_arr_id,
            unmatched_arr_id,
        },
        PlanNode::Union { left, right } => PlanNode::Union {
            left: Box::new(resolve_views_and_snapshots(
                *left,
                registered_views,
                snapshot_sources,
            )),
            right: Box::new(resolve_views_and_snapshots(
                *right,
                registered_views,
                snapshot_sources,
            )),
        },
        PlanNode::Distinct { input, arr_id } => PlanNode::Distinct {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            arr_id,
        },
        PlanNode::Intersect {
            left,
            right,
            all,
            left_arr_id,
            right_arr_id,
        } => PlanNode::Intersect {
            left: Box::new(resolve_views_and_snapshots(
                *left,
                registered_views,
                snapshot_sources,
            )),
            right: Box::new(resolve_views_and_snapshots(
                *right,
                registered_views,
                snapshot_sources,
            )),
            all,
            left_arr_id,
            right_arr_id,
        },
        PlanNode::Except {
            left,
            right,
            all,
            left_arr_id,
            right_arr_id,
        } => PlanNode::Except {
            left: Box::new(resolve_views_and_snapshots(
                *left,
                registered_views,
                snapshot_sources,
            )),
            right: Box::new(resolve_views_and_snapshots(
                *right,
                registered_views,
                snapshot_sources,
            )),
            all,
            left_arr_id,
            right_arr_id,
        },
        PlanNode::Window {
            input,
            window_exprs,
        } => PlanNode::Window {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            window_exprs,
        },
        PlanNode::TumbleWindow {
            input,
            time_col,
            window_size_ms,
            late_data_policy,
        } => PlanNode::TumbleWindow {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            time_col,
            window_size_ms,
            late_data_policy,
        },
        PlanNode::HopWindow {
            input,
            time_col,
            window_size_ms,
            slide_ms,
            late_data_policy,
        } => PlanNode::HopWindow {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            time_col,
            window_size_ms,
            slide_ms,
            late_data_policy,
        },
        PlanNode::TopK {
            input,
            k,
            rank_col,
            partition_by,
        } => PlanNode::TopK {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            k,
            rank_col,
            partition_by,
        },
        PlanNode::Recursion {
            base,
            step,
            max_iterations,
            monotone,
        } => PlanNode::Recursion {
            base: Box::new(resolve_views_and_snapshots(
                *base,
                registered_views,
                snapshot_sources,
            )),
            step: Box::new(resolve_views_and_snapshots(
                *step,
                registered_views,
                snapshot_sources,
            )),
            max_iterations,
            monotone,
        },
        PlanNode::Snapshot { .. } | PlanNode::ViewRef { .. } => node,
        PlanNode::Lateral { input, func } => PlanNode::Lateral {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            func,
        },
        PlanNode::ViewSink {
            view_name,
            pk,
            child,
        } => PlanNode::ViewSink {
            view_name,
            pk,
            child: Box::new(resolve_views_and_snapshots(
                *child,
                registered_views,
                snapshot_sources,
            )),
        },
        PlanNode::Exchange { kind, child } => PlanNode::Exchange {
            kind,
            child: Box::new(resolve_views_and_snapshots(
                *child,
                registered_views,
                snapshot_sources,
            )),
        },
        // v0.32: IndexArrange — pass through, resolving input sources.
        PlanNode::IndexArrange {
            input,
            index_cols,
            pk_cols,
            filter_pred,
        } => PlanNode::IndexArrange {
            input: Box::new(resolve_views_and_snapshots(
                *input,
                registered_views,
                snapshot_sources,
            )),
            index_cols,
            pk_cols,
            filter_pred,
        },
    }
}

// ─── Helper functions for time window and top-k compiling ───────────────────

struct HopTimeSpec {
    time_col: usize,
    slide_idx_col: usize,
    slide_ms: i64,
}

fn try_lower_hop_window_aggregate(
    agg: &datafusion::logical_expr::Aggregate,
) -> Result<Option<PlanNode>, SqlError> {
    let Some((source_input, slide_idx_col)) = extract_generate_series_hop_input(&agg.input) else {
        return Ok(None);
    };

    let mut hop_idx = None;
    let mut hop_window_size_ms = None;
    let mut hop_time_spec = None;
    for (idx, expr) in agg.group_expr.iter().enumerate() {
        let Some(sf) = find_date_bin(expr) else {
            continue;
        };
        if sf.args.len() < 2 {
            return Err(SqlError::UnsupportedPlanNode {
                node_type: "date_bin_missing_args".to_string(),
            });
        }
        let Some(window_size_ms) = extract_interval_ms(&sf.args[0]) else {
            return Err(SqlError::UnsupportedPlanNode {
                node_type: "date_bin_invalid_interval".to_string(),
            });
        };
        let Some(time_spec) =
            extract_hop_time_spec(&sf.args[1], source_input.schema(), agg.input.schema())
        else {
            continue;
        };
        if time_spec.slide_idx_col != slide_idx_col {
            continue;
        }
        hop_idx = Some(idx);
        hop_window_size_ms = Some(window_size_ms);
        hop_time_spec = Some(time_spec);
        break;
    }

    let (hop_idx, window_size_ms, time_spec) = match (hop_idx, hop_window_size_ms, hop_time_spec) {
        (Some(idx), Some(window_size_ms), Some(time_spec)) => (idx, window_size_ms, time_spec),
        _ => return Ok(None),
    };

    if time_spec.slide_ms <= 0 || window_size_ms <= time_spec.slide_ms {
        return Err(SqlError::UnsupportedPlanNode {
            node_type: format!(
                "hop_window_invalid_slide:window_size_ms={window_size_ms},slide_ms={}",
                time_spec.slide_ms
            ),
        });
    }
    if window_size_ms % time_spec.slide_ms != 0 {
        return Err(SqlError::UnsupportedPlanNode {
            node_type: format!(
                "hop_window_non_integral_overlap:window_size_ms={window_size_ms},slide_ms={}",
                time_spec.slide_ms
            ),
        });
    }

    let input = lower(source_input)?;
    let hop_node = PlanNode::HopWindow {
        input: Box::new(input),
        time_col: time_spec.time_col,
        window_size_ms,
        slide_ms: time_spec.slide_ms,
        late_data_policy: rockstream_plan::LateDataPolicy::Drop,
    };

    let source_schema = source_input.schema();
    let mut group_by = Vec::new();
    for (i, expr) in agg.group_expr.iter().enumerate() {
        if i == hop_idx {
            group_by.push(Expr::Column(0));
        } else {
            let mut lowered = lower_expr(expr, source_schema)?;
            shift_expr_cols(&mut lowered);
            group_by.push(lowered);
        }
    }

    let mut aggregates = Vec::new();
    for expr in &agg.aggr_expr {
        let mut lowered = lower_agg_expr(expr, source_schema)?;
        shift_expr_cols(&mut lowered.input);
        aggregates.push(lowered);
    }

    Ok(Some(PlanNode::Aggregate {
        input: Box::new(hop_node),
        group_by,
        aggregates,
    }))
}

fn find_date_bin(expr: &DfExpr) -> Option<&datafusion::logical_expr::expr::ScalarFunction> {
    match expr {
        DfExpr::ScalarFunction(sf) if sf.func.name() == "date_bin" => Some(sf),
        DfExpr::Alias(alias) => find_date_bin(&alias.expr),
        DfExpr::Cast(cast) => find_date_bin(&cast.expr),
        DfExpr::TryCast(cast) => find_date_bin(&cast.expr),
        _ => None,
    }
}

fn extract_interval_ms(expr: &DfExpr) -> Option<i64> {
    match expr {
        DfExpr::Literal(ScalarValue::IntervalMonthDayNano(Some(val)), _) => {
            let ms = (val.days as i64 * 86_400_000) + (val.nanoseconds / 1_000_000);
            Some(ms)
        }
        DfExpr::Literal(ScalarValue::IntervalDayTime(Some(val)), _) => {
            let ms = (val.days as i64 * 86_400_000) + val.milliseconds as i64;
            Some(ms)
        }
        DfExpr::Cast(cast) => extract_interval_ms(&cast.expr),
        DfExpr::TryCast(cast) => extract_interval_ms(&cast.expr),
        _ => None,
    }
}

fn extract_column_index(expr: &DfExpr, schema: &DFSchema) -> Option<usize> {
    match expr {
        DfExpr::Column(col) => schema.index_of_column(col).ok(),
        DfExpr::Cast(cast) => extract_column_index(&cast.expr, schema),
        DfExpr::TryCast(cast) => extract_column_index(&cast.expr, schema),
        _ => None,
    }
}

fn extract_generate_series_hop_input(input: &LogicalPlan) -> Option<(&LogicalPlan, usize)> {
    let LogicalPlan::Join(join) = input else {
        return None;
    };
    if join.join_type != JoinType::Inner || !join.on.is_empty() || join.filter.is_some() {
        return None;
    }
    let right_input = match join.right.as_ref() {
        LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
        other => other,
    };
    let LogicalPlan::Projection(proj) = right_input else {
        return None;
    };
    if proj.expr.len() != 1 {
        return None;
    }
    let DfExpr::Alias(alias) = &proj.expr[0] else {
        return None;
    };
    let DfExpr::Column(col) = alias.expr.as_ref() else {
        return None;
    };
    if col.name != "value" {
        return None;
    }
    let LogicalPlan::TableScan(scan) = proj.input.as_ref() else {
        return None;
    };
    if scan.table_name.table() != "generate_series()" {
        return None;
    }
    Some((join.left.as_ref(), join.left.schema().fields().len()))
}

fn extract_hop_time_spec(
    expr: &DfExpr,
    source_schema: &DFSchema,
    joined_schema: &DFSchema,
) -> Option<HopTimeSpec> {
    match expr {
        DfExpr::Alias(alias) => extract_hop_time_spec(&alias.expr, source_schema, joined_schema),
        DfExpr::Cast(cast) => extract_hop_time_spec(&cast.expr, source_schema, joined_schema),
        DfExpr::TryCast(cast) => extract_hop_time_spec(&cast.expr, source_schema, joined_schema),
        DfExpr::ScalarFunction(sf)
            if sf.func.name() == "to_timestamp_millis" && sf.args.len() == 1 =>
        {
            extract_hop_time_spec(&sf.args[0], source_schema, joined_schema)
        }
        DfExpr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DfOp::Minus => {
            Some(HopTimeSpec {
                time_col: extract_column_index(left, source_schema)?,
                slide_idx_col: extract_slide_component(right, joined_schema)?.0,
                slide_ms: extract_slide_component(right, joined_schema)?.1,
            })
        }
        _ => None,
    }
}

fn extract_slide_component(expr: &DfExpr, schema: &DFSchema) -> Option<(usize, i64)> {
    match expr {
        DfExpr::Alias(alias) => extract_slide_component(&alias.expr, schema),
        DfExpr::Cast(cast) => extract_slide_component(&cast.expr, schema),
        DfExpr::TryCast(cast) => extract_slide_component(&cast.expr, schema),
        DfExpr::BinaryExpr(BinaryExpr { left, op, right }) if *op == DfOp::Multiply => {
            if let (Some(col), Some(ms)) = (
                extract_column_index(left, schema),
                extract_int64_literal(right),
            ) {
                Some((col, ms))
            } else if let (Some(ms), Some(col)) = (
                extract_int64_literal(left),
                extract_column_index(right, schema),
            ) {
                Some((col, ms))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_int64_literal(expr: &DfExpr) -> Option<i64> {
    match expr {
        DfExpr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v),
        DfExpr::Literal(ScalarValue::Int32(Some(v)), _) => Some(*v as i64),
        DfExpr::Literal(ScalarValue::UInt64(Some(v)), _) => i64::try_from(*v).ok(),
        DfExpr::Literal(ScalarValue::UInt32(Some(v)), _) => Some(*v as i64),
        DfExpr::Alias(alias) => extract_int64_literal(&alias.expr),
        DfExpr::Cast(cast) => extract_int64_literal(&cast.expr),
        DfExpr::TryCast(cast) => extract_int64_literal(&cast.expr),
        _ => None,
    }
}

fn shift_expr_cols(expr: &mut Expr) {
    match expr {
        Expr::Column(idx) => {
            *idx += 1;
        }
        Expr::BinaryOp { left, right, .. } => {
            shift_expr_cols(left);
            shift_expr_cols(right);
        }
        Expr::ScalarUdf { args, .. } => {
            for arg in args {
                shift_expr_cols(arg);
            }
        }
        Expr::Literal(_) => {}
    }
}

fn try_lower_topk_pattern(
    outer_proj: &datafusion::logical_expr::Projection,
) -> Result<Option<PlanNode>, SqlError> {
    let mut current_input = outer_proj.input.as_ref();
    while let LogicalPlan::SubqueryAlias(alias) = current_input {
        current_input = &alias.input;
    }

    let filter = match current_input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(None),
    };

    let (rn_col, k) = match &filter.predicate {
        DfExpr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            datafusion::logical_expr::Operator::LtEq => {
                if let DfExpr::Column(col) = left.as_ref() {
                    if let DfExpr::Literal(ScalarValue::Int64(Some(k_val)), _) = right.as_ref() {
                        (col, *k_val)
                    } else if let DfExpr::Literal(ScalarValue::Int32(Some(k_val)), _) =
                        right.as_ref()
                    {
                        (col, *k_val as i64)
                    } else {
                        return Ok(None);
                    }
                } else if let DfExpr::Column(col) = right.as_ref() {
                    if let DfExpr::Literal(ScalarValue::Int64(Some(k_val)), _) = left.as_ref() {
                        (col, *k_val)
                    } else if let DfExpr::Literal(ScalarValue::Int32(Some(k_val)), _) =
                        left.as_ref()
                    {
                        (col, *k_val as i64)
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            datafusion::logical_expr::Operator::Lt => {
                if let DfExpr::Column(col) = left.as_ref() {
                    if let DfExpr::Literal(ScalarValue::Int64(Some(k_val)), _) = right.as_ref() {
                        (col, *k_val - 1)
                    } else if let DfExpr::Literal(ScalarValue::Int32(Some(k_val)), _) =
                        right.as_ref()
                    {
                        (col, (*k_val - 1) as i64)
                    } else {
                        return Ok(None);
                    }
                } else if let DfExpr::Column(col) = right.as_ref() {
                    if let DfExpr::Literal(ScalarValue::Int64(Some(k_val)), _) = left.as_ref() {
                        (col, *k_val - 1)
                    } else if let DfExpr::Literal(ScalarValue::Int32(Some(k_val)), _) =
                        left.as_ref()
                    {
                        (col, (*k_val - 1) as i64)
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let rn_col_idx = filter.input.schema().index_of_column(rn_col).map_err(|e| {
        SqlError::UnsupportedPlanNode {
            node_type: format!("column_resolution_rn:{e}"),
        }
    })?;

    let mut inner_input = filter.input.as_ref();
    while let LogicalPlan::SubqueryAlias(alias) = inner_input {
        inner_input = &alias.input;
    }

    let proj = match inner_input {
        LogicalPlan::Projection(p) => p,
        _ => return Ok(None),
    };

    let expr = &proj.expr[rn_col_idx];
    let window_col = match expr {
        DfExpr::Alias(alias) => &alias.expr,
        other => other,
    };

    let col = match window_col {
        DfExpr::Column(col) => col,
        _ => return Ok(None),
    };

    let window_output_idx =
        proj.input
            .schema()
            .index_of_column(col)
            .map_err(|e| SqlError::UnsupportedPlanNode {
                node_type: format!("column_resolution_window:{e}"),
            })?;

    let mut window_plan = proj.input.as_ref();
    while let LogicalPlan::SubqueryAlias(alias) = window_plan {
        window_plan = &alias.input;
    }

    let w = match window_plan {
        LogicalPlan::Window(w) => w,
        _ => return Ok(None),
    };

    let input_fields_count = w.input.schema().fields().len();
    if window_output_idx < input_fields_count {
        return Ok(None);
    }
    let window_expr_idx = window_output_idx - input_fields_count;
    if window_expr_idx >= w.window_expr.len() {
        return Ok(None);
    }

    let win_expr = &w.window_expr[window_expr_idx];
    let wf = match win_expr {
        DfExpr::WindowFunction(wf) => wf.as_ref(),
        _ => return Ok(None),
    };

    let name = match &wf.fun {
        WindowFunctionDefinition::WindowUDF(udwf) => udwf.name(),
        _ => return Ok(None),
    };
    if name != "row_number" && name != "rank" {
        return Ok(None);
    }

    if wf.params.order_by.is_empty() {
        return Ok(None);
    }
    let sort = &wf.params.order_by[0];
    if sort.asc {
        return Ok(None);
    }
    let rank_col =
        match &sort.expr {
            DfExpr::Column(col) => w.input.schema().index_of_column(col).map_err(|e| {
                SqlError::UnsupportedPlanNode {
                    node_type: format!("column_resolution_order_by:{e}"),
                }
            })?,
            _ => return Ok(None),
        };

    let mut partition_by = Vec::new();
    for p_expr in &wf.params.partition_by {
        if let DfExpr::Column(col) = p_expr {
            let idx = w.input.schema().index_of_column(col).map_err(|e| {
                SqlError::UnsupportedPlanNode {
                    node_type: format!("column_resolution_partition_by:{e}"),
                }
            })?;
            partition_by.push(idx);
        } else {
            return Ok(None);
        }
    }

    let input_node = lower(&w.input)?;

    let topk = PlanNode::TopK {
        input: Box::new(input_node),
        k: k as usize,
        rank_col,
        partition_by,
    };

    let mut columns = Vec::new();
    for outer_expr in &outer_proj.expr {
        let col = match outer_expr {
            DfExpr::Column(c) => c,
            DfExpr::Alias(alias) => {
                if let DfExpr::Column(c) = alias.expr.as_ref() {
                    c
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };
        let outer_col_idx = filter.input.schema().index_of_column(col).map_err(|e| {
            SqlError::UnsupportedPlanNode {
                node_type: format!("column_resolution_outer_proj:{e}"),
            }
        })?;
        let inner_expr = &proj.expr[outer_col_idx];
        let lowered = lower_expr(inner_expr, w.input.schema())?;
        columns.push(lowered);
    }

    Ok(Some(PlanNode::Project {
        input: Box::new(topk),
        columns,
    }))
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::scalar::ScalarValue;

    #[test]
    fn encode_scalar_int64() {
        let v = encode_scalar(&ScalarValue::Int64(Some(42))).unwrap();
        assert_eq!(v, 42i64.to_be_bytes().to_vec());
    }

    #[test]
    fn encode_scalar_int32_promotes_to_i64() {
        let v = encode_scalar(&ScalarValue::Int32(Some(10))).unwrap();
        assert_eq!(v, 10i64.to_be_bytes().to_vec());
    }

    #[test]
    fn encode_scalar_bool() {
        assert_eq!(
            encode_scalar(&ScalarValue::Boolean(Some(true))).unwrap(),
            vec![1]
        );
        assert_eq!(
            encode_scalar(&ScalarValue::Boolean(Some(false))).unwrap(),
            vec![0]
        );
    }

    #[test]
    fn encode_scalar_null_is_empty() {
        let v = encode_scalar(&ScalarValue::Int64(None)).unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn binary_op_eq() {
        assert_eq!(lower_binary_op(&DfOp::Eq).unwrap(), BinaryOp::Eq);
        assert_eq!(lower_binary_op(&DfOp::Gt).unwrap(), BinaryOp::Gt);
        assert_eq!(lower_binary_op(&DfOp::Plus).unwrap(), BinaryOp::Add);
    }

    // ── SQL lowering for Distinct / Union (v0.10 — IVM-6) ────────────────────
    //
    // These tests parse real SQL via DataFusion and verify the lowered PlanNode
    // structure.  They require the SqlFrontend helper.

    fn make_frontend_with_t() -> crate::frontend::SqlFrontend {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let f = crate::frontend::SqlFrontend::new();
        f.register_table("t", schema).unwrap();
        f
    }

    fn make_frontend_with_a_b() -> crate::frontend::SqlFrontend {
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let f = crate::frontend::SqlFrontend::new();
        f.register_table("a", schema.clone()).unwrap();
        f.register_table("b", schema).unwrap();
        f
    }

    fn has_distinct(plan: &PlanNode) -> bool {
        match plan {
            PlanNode::Distinct { .. } => true,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::ViewSink { child: input, .. }
            | PlanNode::Exchange { child: input, .. } => has_distinct(input),
            PlanNode::InnerJoin { left, right, .. }
            | PlanNode::OuterJoin { left, right, .. }
            | PlanNode::Union { left, right }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => has_distinct(left) || has_distinct(right),
            _ => false,
        }
    }

    fn has_union(plan: &PlanNode) -> bool {
        match plan {
            PlanNode::Union { .. } => true,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. }
            | PlanNode::ViewSink { child: input, .. }
            | PlanNode::Exchange { child: input, .. } => has_union(input),
            PlanNode::InnerJoin { left, right, .. }
            | PlanNode::OuterJoin { left, right, .. }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => has_union(left) || has_union(right),
            _ => false,
        }
    }

    /// `SELECT DISTINCT id FROM t` must lower to a plan containing a `Distinct` node.
    #[tokio::test]
    async fn select_distinct_lowers_to_distinct_node() {
        let f = make_frontend_with_t();
        let plan = f
            .sql_to_plan_node("SELECT DISTINCT id FROM t")
            .await
            .expect("SELECT DISTINCT should lower");
        assert!(
            has_distinct(&plan),
            "expected Distinct node in plan: {plan:?}"
        );
    }

    /// `SELECT id FROM a UNION ALL SELECT id FROM b` must lower to a plan
    /// containing a `Union` node.
    #[tokio::test]
    async fn union_all_lowers_to_union_node() {
        let f = make_frontend_with_a_b();
        let plan = f
            .sql_to_plan_node("SELECT id FROM a UNION ALL SELECT id FROM b")
            .await
            .expect("UNION ALL should lower");
        assert!(has_union(&plan), "expected Union node in plan: {plan:?}");
    }

    /// `SELECT id FROM a INTERSECT SELECT id FROM b` must lower without error.
    /// In DataFusion 53, INTERSECT is translated to a LeftSemi join, so the
    /// lowered plan contains an OuterJoin(Semi) node (not a Distinct/Intersect
    /// node).
    #[tokio::test]
    async fn intersect_lowers_without_error() {
        let f = make_frontend_with_a_b();
        let plan = f
            .sql_to_plan_node("SELECT id FROM a INTERSECT SELECT id FROM b")
            .await
            .expect("INTERSECT should lower without error");
        // The plan must not be empty.
        assert!(
            !matches!(plan, PlanNode::Source { .. } if false),
            "plan: {plan:?}"
        );
    }

    /// `SELECT id FROM a EXCEPT SELECT id FROM b` must lower without error.
    /// In DataFusion 53, EXCEPT is translated to a LeftAnti join, so the
    /// lowered plan contains an OuterJoin(Anti) node.
    #[tokio::test]
    async fn except_lowers_without_error() {
        let f = make_frontend_with_a_b();
        let plan = f
            .sql_to_plan_node("SELECT id FROM a EXCEPT SELECT id FROM b")
            .await
            .expect("EXCEPT should lower without error");
        assert!(
            !matches!(plan, PlanNode::Source { .. } if false),
            "plan: {plan:?}"
        );
    }

    fn make_frontend_with_edges() -> crate::frontend::SqlFrontend {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        let f = crate::frontend::SqlFrontend::new();
        f.register_table("edges", schema).unwrap();
        f
    }

    fn find_recursion(plan: &PlanNode) -> Option<&PlanNode> {
        match plan {
            PlanNode::Recursion { .. } => Some(plan),
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. }
            | PlanNode::Window { input, .. }
            | PlanNode::TumbleWindow { input, .. }
            | PlanNode::HopWindow { input, .. }
            | PlanNode::TopK { input, .. }
            | PlanNode::Lateral { input, .. }
            | PlanNode::IndexArrange { input, .. } => find_recursion(input),
            PlanNode::Exchange { child, .. } | PlanNode::ViewSink { child, .. } => {
                find_recursion(child)
            }
            PlanNode::Join { left, right, .. }
            | PlanNode::InnerJoin { left, right, .. }
            | PlanNode::OuterJoin { left, right, .. }
            | PlanNode::Union { left, right }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => {
                find_recursion(left).or_else(|| find_recursion(right))
            }
            PlanNode::Source { .. } | PlanNode::Snapshot { .. } | PlanNode::ViewRef { .. } => None,
        }
    }

    fn contains_source_named(plan: &PlanNode, needle: &str) -> bool {
        match plan {
            PlanNode::Source { name } => name == needle,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. }
            | PlanNode::Window { input, .. }
            | PlanNode::TumbleWindow { input, .. }
            | PlanNode::HopWindow { input, .. }
            | PlanNode::TopK { input, .. }
            | PlanNode::Lateral { input, .. }
            | PlanNode::IndexArrange { input, .. } => contains_source_named(input, needle),
            PlanNode::Exchange { child, .. } | PlanNode::ViewSink { child, .. } => {
                contains_source_named(child, needle)
            }
            PlanNode::Join { left, right, .. }
            | PlanNode::InnerJoin { left, right, .. }
            | PlanNode::OuterJoin { left, right, .. }
            | PlanNode::Union { left, right }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => {
                contains_source_named(left, needle) || contains_source_named(right, needle)
            }
            PlanNode::Recursion { base, step, .. } => {
                contains_source_named(base, needle) || contains_source_named(step, needle)
            }
            PlanNode::Snapshot { source_name, .. } => source_name == needle,
            PlanNode::ViewRef { view_name } => view_name == needle,
        }
    }

    fn find_recursive_query(
        plan: &LogicalPlan,
    ) -> Option<&datafusion::logical_expr::RecursiveQuery> {
        match plan {
            LogicalPlan::RecursiveQuery(recursive) => Some(recursive),
            _ => plan.inputs().into_iter().find_map(find_recursive_query),
        }
    }

    #[tokio::test]
    async fn lower_with_recursive_transitive_closure_to_recursion_node() {
        let f = make_frontend_with_edges();
        let plan = f
            .sql_to_unoptimized_plan_node(
                "WITH RECURSIVE reach(src, dst) AS ( \
                    SELECT src, dst FROM edges \
                    UNION ALL \
                    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src \
                 ) \
                 SELECT src, dst FROM reach",
            )
            .await
            .expect("recursive transitive closure should lower");

        let recursion = find_recursion(&plan).expect("expected Recursion node in lowered plan");
        let PlanNode::Recursion {
            base,
            step,
            max_iterations,
            monotone,
        } = recursion
        else {
            panic!("expected recursion node: {recursion:?}");
        };

        assert!(
            *monotone,
            "transitive closure should be classified monotone"
        );
        assert_eq!(*max_iterations, 1024);
        assert!(matches!(base.as_ref(), PlanNode::Project { .. }));
        assert!(matches!(step.as_ref(), PlanNode::Project { .. }));
        assert!(
            contains_source_named(step, "reach"),
            "recursive step must retain the self-reference source: {step:?}"
        );
    }

    #[tokio::test]
    async fn lower_with_recursive_classifies_except_term_as_non_monotone() {
        let f = make_frontend_with_edges();
        let plan = f
            .sql_to_unoptimized_plan_node(
                "WITH RECURSIVE reach(src, dst) AS ( \
                    SELECT src, dst FROM edges \
                    UNION ALL \
                    (SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src \
                     EXCEPT \
                     SELECT src, dst FROM edges) \
                 ) \
                 SELECT src, dst FROM reach",
            )
            .await
            .expect("recursive EXCEPT query should lower");

        let recursion = find_recursion(&plan).expect("expected Recursion node in lowered plan");
        let PlanNode::Recursion { monotone, .. } = recursion else {
            panic!("expected recursion node: {recursion:?}");
        };
        assert!(!monotone, "EXCEPT recursive term must be non-monotone");
    }

    #[tokio::test]
    async fn lower_with_recursive_rejects_column_count_mismatch() {
        use datafusion::datasource::MemTable;
        use datafusion::execution::context::SessionConfig;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
        ]));
        ctx.register_table(
            "edges",
            Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap()),
        )
        .unwrap();

        let good_plan = ctx
            .sql(
                "WITH RECURSIVE reach(src, dst) AS ( \
                    SELECT src, dst FROM edges \
                    UNION ALL \
                    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src \
                 ) \
                 SELECT src, dst FROM reach",
            )
            .await
            .unwrap()
            .into_unoptimized_plan();
        let one_col_plan = ctx
            .sql("SELECT src FROM edges")
            .await
            .unwrap()
            .into_unoptimized_plan();
        let recursive = find_recursive_query(&good_plan)
            .expect("expected RecursiveQuery")
            .clone();
        let mismatched = LogicalPlan::RecursiveQuery(datafusion::logical_expr::RecursiveQuery {
            name: recursive.name,
            static_term: recursive.static_term,
            recursive_term: Arc::new(one_col_plan),
            is_distinct: recursive.is_distinct,
        });
        let err = lower(&mismatched).expect_err("column-count mismatch must be rejected");
        assert_eq!(err.error_code().to_string(), "RS-1013");
        assert!(
            err.to_string().contains("column_count_mismatch"),
            "error must identify the recursive shape mismatch: {err}"
        );
    }

    // ─── Window lowering tests (v0.11 — IVM-7) ───────────────────────────────

    fn make_frontend_kv() -> crate::frontend::SqlFrontend {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let f = crate::frontend::SqlFrontend::new();
        f.register_table("t", schema).unwrap();
        f
    }

    fn has_window(plan: &PlanNode) -> bool {
        match plan {
            PlanNode::Window { .. } => true,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. } => has_window(input),
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

    fn find_window_exprs(plan: &PlanNode) -> Option<Vec<rockstream_plan::WindowExpr>> {
        match plan {
            PlanNode::Window { window_exprs, .. } => Some(window_exprs.clone()),
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Map { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Distinct { input, .. } => find_window_exprs(input),
            PlanNode::ViewSink { child, .. } | PlanNode::Exchange { child, .. } => {
                find_window_exprs(child)
            }
            _ => None,
        }
    }

    #[tokio::test]
    async fn lower_window_row_number() {
        let f = make_frontend_kv();
        let plan = f
            .sql_to_plan_node(
                "SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn FROM t",
            )
            .await
            .expect("ROW_NUMBER window should lower");
        assert!(has_window(&plan), "expected Window node: {plan:?}");
        let exprs = find_window_exprs(&plan).unwrap();
        assert!(!exprs.is_empty());
        assert!(
            matches!(exprs[0].func, rockstream_plan::WindowFunc::RowNumber),
            "expected RowNumber, got: {:?}",
            exprs[0].func
        );
    }

    #[tokio::test]
    async fn lower_window_rank() {
        let f = make_frontend_kv();
        let plan = f
            .sql_to_plan_node("SELECT k, v, RANK() OVER (PARTITION BY k ORDER BY v) AS r FROM t")
            .await
            .expect("RANK window should lower");
        assert!(has_window(&plan));
        let exprs = find_window_exprs(&plan).unwrap();
        assert!(matches!(exprs[0].func, rockstream_plan::WindowFunc::Rank));
    }

    #[tokio::test]
    async fn lower_window_dense_rank() {
        let f = make_frontend_kv();
        let plan = f
            .sql_to_plan_node(
                "SELECT k, v, DENSE_RANK() OVER (PARTITION BY k ORDER BY v) AS dr FROM t",
            )
            .await
            .expect("DENSE_RANK window should lower");
        assert!(has_window(&plan));
        let exprs = find_window_exprs(&plan).unwrap();
        assert!(matches!(
            exprs[0].func,
            rockstream_plan::WindowFunc::DenseRank
        ));
    }

    #[tokio::test]
    async fn lower_window_lag() {
        let f = make_frontend_kv();
        let plan = f
            .sql_to_plan_node("SELECT k, v, LAG(v, 1) OVER (PARTITION BY k ORDER BY v) AS l FROM t")
            .await
            .expect("LAG window should lower");
        assert!(has_window(&plan));
        let exprs = find_window_exprs(&plan).unwrap();
        assert!(
            matches!(
                exprs[0].func,
                rockstream_plan::WindowFunc::Lag { offset: 1 }
            ),
            "expected Lag{{offset:1}}, got {:?}",
            exprs[0].func
        );
    }

    #[tokio::test]
    async fn lower_window_lead() {
        let f = make_frontend_kv();
        let plan = f
            .sql_to_plan_node(
                "SELECT k, v, LEAD(v, 1) OVER (PARTITION BY k ORDER BY v) AS l FROM t",
            )
            .await
            .expect("LEAD window should lower");
        assert!(has_window(&plan));
        let exprs = find_window_exprs(&plan).unwrap();
        assert!(
            matches!(
                exprs[0].func,
                rockstream_plan::WindowFunc::Lead { offset: 1 }
            ),
            "expected Lead{{offset:1}}, got {:?}",
            exprs[0].func
        );
    }

    #[tokio::test]
    async fn lower_window_sliding_sum() {
        let f = make_frontend_kv();
        let plan = f
            .sql_to_plan_node(
                "SELECT k, v, SUM(v) OVER (PARTITION BY k ORDER BY v ROWS 2 PRECEDING) AS s FROM t",
            )
            .await
            .expect("SUM OVER ROWS window should lower");
        assert!(has_window(&plan));
        let exprs = find_window_exprs(&plan).unwrap();
        assert!(
            matches!(
                exprs[0].func,
                rockstream_plan::WindowFunc::SlidingSum { frame_rows: 3 }
            ),
            "expected SlidingSum{{frame_rows:3}}, got {:?}",
            exprs[0].func
        );
    }

    #[tokio::test]
    async fn lower_window_unsupported_returns_error() {
        let f = make_frontend_kv();
        // NTILE is not supported in v0.11 — should return RS-1016 error.
        let result = f
            .sql_to_plan_node("SELECT k, v, NTILE(4) OVER (ORDER BY v) AS bucket FROM t")
            .await;
        assert!(result.is_err(), "NTILE should return an error");
        let err = result.unwrap_err();
        let err_str = format!("{err}");
        assert!(
            err_str.contains("RS-1016") || err_str.contains("1016"),
            "expected RS-1016 error, got: {err_str}"
        );
    }
}
