use crate::{SqlError, SqlFrontend};
use datafusion::logical_expr::Operator as DFOperator;
use datafusion::logical_expr::{
    expr::{AggregateFunction as DFAggFunc, Alias, BinaryExpr, Case, Cast},
    Expr as DFExpr, LogicalPlan,
};
use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr as PlanExpr, PlanNode};

impl SqlFrontend {
    /// Lower a DataFusion `LogicalPlan` to a RockStream `PlanNode` tree.
    pub fn lower(&self, plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        self.lower_plan(plan)
    }

    pub(crate) fn lower_plan(&self, plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        match plan {
            LogicalPlan::TableScan(ts) => Ok(PlanNode::Source {
                name: ts.table_name.table().to_string(),
            }),

            LogicalPlan::EmptyRelation(_) => Ok(PlanNode::Source {
                name: "<empty>".into(),
            }),

            LogicalPlan::Projection(proj) => {
                let input = self.lower_plan(proj.input.as_ref())?;
                let columns = proj
                    .expr
                    .iter()
                    .map(|e| self.lower_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanNode::Project {
                    input: Box::new(input),
                    columns,
                })
            }

            LogicalPlan::Filter(filter) => {
                let input = self.lower_plan(filter.input.as_ref())?;
                let predicate = self.lower_expr(&filter.predicate)?;
                Ok(PlanNode::Filter {
                    input: Box::new(input),
                    predicate,
                })
            }

            LogicalPlan::Aggregate(agg) => {
                let input = self.lower_plan(agg.input.as_ref())?;
                let group_by = agg
                    .group_expr
                    .iter()
                    .map(|e| self.lower_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                let aggregates = agg
                    .aggr_expr
                    .iter()
                    .map(|e| self.lower_aggregate_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanNode::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggregates,
                })
            }

            LogicalPlan::Union(union_plan) => {
                let mut it = union_plan.inputs.iter();
                let first = match it.next() {
                    Some(p) => self.lower_plan(p)?,
                    None => {
                        return Err(SqlError::Resolution("Union has no inputs".into()));
                    }
                };
                it.try_fold(first, |acc, next| {
                    let right = self.lower_plan(next)?;
                    Ok(PlanNode::Union {
                        left: Box::new(acc),
                        right: Box::new(right),
                    })
                })
            }

            // Inner/outer/cross joins.
            LogicalPlan::Join(join) => {
                let left = self.lower_plan(join.left.as_ref())?;
                let right = self.lower_plan(join.right.as_ref())?;
                let condition = self.lower_join_condition(&join.on, join.filter.as_ref())?;
                Ok(PlanNode::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                })
            }

            // SubqueryAlias is transparent — lower the inner plan.
            LogicalPlan::SubqueryAlias(alias) => self.lower_plan(alias.input.as_ref()),

            LogicalPlan::Window(window) => {
                let input = self.lower_plan(window.input.as_ref())?;
                let mut window_exprs = Vec::new();
                for expr in &window.window_expr {
                    if let DFExpr::WindowFunction(window_fun) = expr {
                        let name = match &window_fun.fun {
                            datafusion::logical_expr::WindowFunctionDefinition::WindowUDF(udf) => {
                                udf.name().to_lowercase()
                            }
                            datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                                udf,
                            ) => udf.name().to_lowercase(),
                        };
                        let window_func = match name.as_str() {
                            "row_number" => rockstream_plan::WindowFunc::RowNumber,
                            "rank" => rockstream_plan::WindowFunc::Rank,
                            "dense_rank" => rockstream_plan::WindowFunc::DenseRank,
                            "lag" => rockstream_plan::WindowFunc::Lag { offset: 1 },
                            "lead" => rockstream_plan::WindowFunc::Lead { offset: 1 },
                            _ => rockstream_plan::WindowFunc::RowNumber,
                        };
                        let partition_by_indices = window_fun
                            .params
                            .partition_by
                            .iter()
                            .map(|e| self.resolve_col_index(window.input.schema(), e))
                            .collect::<Vec<_>>();
                        let order_by_indices = window_fun
                            .params
                            .order_by
                            .iter()
                            .map(|se| self.resolve_col_index(window.input.schema(), &se.expr))
                            .collect::<Vec<_>>();
                        window_exprs.push(rockstream_plan::WindowExpr {
                            func: window_func,
                            partition_by: partition_by_indices,
                            order_by: order_by_indices,
                        });
                    }
                }
                Ok(PlanNode::Window {
                    input: Box::new(input),
                    window_exprs,
                })
            }

            // Limit/Sort/Distinct are not yet supported; return a clear error.
            other => Err(SqlError::NotYetImplemented(format!(
                "LogicalPlan node '{}' — will be lowered in v0.12+",
                other.display()
            ))),
        }
    }

    fn lower_join_condition(
        &self,
        on: &[(DFExpr, DFExpr)],
        filter: Option<&DFExpr>,
    ) -> Result<PlanExpr, SqlError> {
        if !on.is_empty() {
            let mut result: Option<PlanExpr> = None;
            for (l, r) in on {
                let le = self.lower_expr(l)?;
                let re = self.lower_expr(r)?;
                let pair = PlanExpr::BinaryOp {
                    op: BinaryOp::Eq,
                    left: Box::new(le),
                    right: Box::new(re),
                };
                result = Some(match result {
                    None => pair,
                    Some(acc) => PlanExpr::BinaryOp {
                        op: BinaryOp::And,
                        left: Box::new(acc),
                        right: Box::new(pair),
                    },
                });
            }
            Ok(result.unwrap())
        } else if let Some(f) = filter {
            self.lower_expr(f)
        } else {
            // Cross join — condition is always-true literal `[1]`.
            Ok(PlanExpr::Literal(vec![1u8]))
        }
    }

    fn lower_expr(&self, expr: &DFExpr) -> Result<PlanExpr, SqlError> {
        match expr {
            DFExpr::Column(_col) => Ok(PlanExpr::Column(0)),

            DFExpr::Literal(scalar, _) => {
                let bytes = scalar.to_string().into_bytes();
                Ok(PlanExpr::Literal(bytes))
            }

            DFExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.lower_expr(left.as_ref())?;
                let r = self.lower_expr(right.as_ref())?;
                let bin_op = self.lower_operator(op)?;
                Ok(PlanExpr::BinaryOp {
                    op: bin_op,
                    left: Box::new(l),
                    right: Box::new(r),
                })
            }

            DFExpr::Cast(Cast { expr, .. }) => self.lower_expr(expr.as_ref()),
            DFExpr::TryCast(tc) => self.lower_expr(tc.expr.as_ref()),

            DFExpr::Case(Case {
                when_then_expr,
                else_expr,
                ..
            }) => {
                if let Some(e) = else_expr {
                    self.lower_expr(e.as_ref())
                } else if let Some((_, result)) = when_then_expr.first() {
                    self.lower_expr(result.as_ref())
                } else {
                    Err(SqlError::NotYetImplemented("CASE with no arms".into()))
                }
            }

            DFExpr::Alias(Alias { expr, .. }) => self.lower_expr(expr.as_ref()),

            other => Err(SqlError::NotYetImplemented(format!("Expr: {other}"))),
        }
    }

    fn lower_operator(&self, op: &DFOperator) -> Result<BinaryOp, SqlError> {
        match op {
            DFOperator::Eq => Ok(BinaryOp::Eq),
            DFOperator::NotEq => Ok(BinaryOp::Ne),
            DFOperator::Lt => Ok(BinaryOp::Lt),
            DFOperator::LtEq => Ok(BinaryOp::Le),
            DFOperator::Gt => Ok(BinaryOp::Gt),
            DFOperator::GtEq => Ok(BinaryOp::Ge),
            DFOperator::Plus => Ok(BinaryOp::Add),
            DFOperator::Minus => Ok(BinaryOp::Sub),
            DFOperator::Multiply => Ok(BinaryOp::Mul),
            DFOperator::Divide => Ok(BinaryOp::Div),
            DFOperator::And => Ok(BinaryOp::And),
            DFOperator::Or => Ok(BinaryOp::Or),
            other => Err(SqlError::NotYetImplemented(format!("Operator: {other:?}"))),
        }
    }

    fn lower_aggregate_expr(&self, expr: &DFExpr) -> Result<AggregateExpr, SqlError> {
        match expr {
            DFExpr::AggregateFunction(DFAggFunc { func, params }) => {
                let name = func.name().to_lowercase();
                let agg_func = match name.as_str() {
                    "count" => AggregateFunc::Count,
                    "sum" => AggregateFunc::Sum,
                    "avg" | "mean" => AggregateFunc::Avg,
                    "min" => AggregateFunc::Min,
                    "max" => AggregateFunc::Max,
                    other => {
                        return Err(SqlError::NotYetImplemented(format!(
                            "aggregate function '{other}'"
                        )));
                    }
                };
                let input_expr = params
                    .args
                    .first()
                    .map(|e| self.lower_expr(e))
                    .unwrap_or(Ok(PlanExpr::Column(0)))?;
                Ok(AggregateExpr {
                    func: agg_func,
                    input: input_expr,
                    distinct: params.distinct,
                })
            }

            DFExpr::Alias(Alias { expr, .. }) => self.lower_aggregate_expr(expr.as_ref()),

            other => Err(SqlError::NotYetImplemented(format!(
                "aggregate expression: {other}"
            ))),
        }
    }

    fn resolve_col_index(&self, schema: &datafusion::common::DFSchemaRef, expr: &DFExpr) -> usize {
        match expr {
            DFExpr::Column(col) => schema.index_of_column(col).unwrap_or(0),
            DFExpr::Alias(Alias { expr, .. }) => self.resolve_col_index(schema, expr),
            _ => 0,
        }
    }
}
