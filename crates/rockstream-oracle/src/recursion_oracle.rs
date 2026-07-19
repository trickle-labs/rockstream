#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionConfig;
    use datafusion::prelude::SessionContext;
    use rockstream_ops::recursion::{DistributedShardStatus, RecursionOp};
    use rockstream_ops::zset::ArrowZSet;
    use rockstream_plan::{Expr, JoinSemantics, OuterJoinKind, PlanNode};
    use rockstream_types::ids::OperatorId;

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

    fn monotone_step() -> PlanNode {
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
                input: Box::new(monotone_step()),
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

    fn accumulate(state: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() {
            return;
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
            *state
                .entry((src.value(row_idx), dst.value(row_idx)))
                .or_insert(0) += batch.weights[row_idx];
        }
        state.retain(|_, weight| *weight > 0);
    }

    async fn datafusion_recursive_query(
        rows: &[(i64, i64)],
        sql: &str,
    ) -> BTreeMap<(i64, i64), i64> {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let src: Vec<i64> = rows.iter().map(|row| row.0).collect();
        let dst: Vec<i64> = rows.iter().map(|row| row.1).collect();
        let batch = RecordBatch::try_new(
            schema_edges(),
            vec![
                Arc::new(Int64Array::from(src)) as ArrayRef,
                Arc::new(Int64Array::from(dst)) as ArrayRef,
            ],
        )
        .unwrap();
        ctx.register_table(
            "edges",
            Arc::new(MemTable::try_new(schema_edges(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        let mut out = BTreeMap::new();
        for batch in batches {
            let src = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let dst = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row_idx in 0..batch.num_rows() {
                out.insert((src.value(row_idx), dst.value(row_idx)), 1);
            }
        }
        out
    }

    #[tokio::test]
    async fn oracle_recursion_transitive_closure() {
        let op = RecursionOp::new(schema_edges(), base_plan(), monotone_step(), 16, true);
        let mut incremental = BTreeMap::new();
        let out = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        accumulate(&mut incremental, &out);
        let batch = datafusion_recursive_query(
            &[(1, 2), (2, 3), (3, 4)],
            "WITH RECURSIVE reach(src, dst) AS ( \
                SELECT src, dst FROM edges \
                UNION ALL \
                SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src \
             ) \
             SELECT DISTINCT src, dst FROM reach",
        )
        .await;
        assert_eq!(incremental, batch);
    }

    #[tokio::test]
    async fn oracle_recursion_non_monotone_recompute_matches_batch() {
        let op = RecursionOp::new(schema_edges(), base_plan(), non_monotone_step(), 16, false);
        let mut incremental = BTreeMap::new();
        let out = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        accumulate(&mut incremental, &out);
        let batch = datafusion_recursive_query(
            &[(1, 2), (2, 3), (3, 4)],
            "WITH RECURSIVE reach(src, dst) AS ( \
                SELECT src, dst FROM edges \
                UNION ALL \
                (SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src \
                 EXCEPT SELECT src, dst FROM edges) \
             ) \
             SELECT DISTINCT src, dst FROM reach",
        )
        .await;
        assert_eq!(incremental, batch);
    }

    #[test]
    fn oracle_distributed_recursion_matches_single_shard() {
        let single = RecursionOp::new(schema_edges(), base_plan(), monotone_step(), 16, true);
        let distributed = RecursionOp::new(schema_edges(), base_plan(), monotone_step(), 16, true);
        let single_out = single
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        let distributed_out = distributed
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
                        iteration_cost: 11,
                    },
                ],
                1,
            )
            .unwrap();
        let mut lhs = BTreeMap::new();
        let mut rhs = BTreeMap::new();
        accumulate(&mut lhs, &single_out);
        accumulate(&mut rhs, &distributed_out);
        assert_eq!(lhs, rhs);
    }
}
