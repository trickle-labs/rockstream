//! Oracle tests for `LateralOp` (v0.50 Track B).
//!
//! DataFusion 53 lowers scalar `UNNEST(tags)` projections to `LogicalPlan::Unnest`,
//! but rejects explicit `CROSS JOIN LATERAL UNNEST(...)` table-function syntax in
//! the vendored grammar. The oracle therefore validates the same row-expansion
//! semantics with the supported scalar-`UNNEST` surface.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, ListArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use rockstream_ops::lateral::LateralOp;
    use rockstream_ops::zset::ArrowZSet;
    use rockstream_plan::LateralFunc;

    type InputRow = (i64, Vec<i64>, i64);

    fn docs_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]))
    }

    fn make_epoch(rows: &[InputRow]) -> ArrowZSet {
        let ids = Int64Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
        let tags = ListArray::from_iter_primitive::<Int64Type, _, _>(
            rows.iter()
                .map(|(_, values, _)| Some(values.iter().copied().map(Some).collect::<Vec<_>>())),
        );
        let weights = rows.iter().map(|(_, _, w)| *w).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            docs_schema(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(tags) as ArrayRef],
        )
        .unwrap();
        ArrowZSet::new(batch, weights)
    }

    fn accumulate_input(state: &mut BTreeMap<(i64, Vec<i64>), i64>, epoch: &[InputRow]) {
        for (id, tags, weight) in epoch {
            let entry = state.entry((*id, tags.clone())).or_insert(0);
            *entry += *weight;
            if *entry == 0 {
                state.remove(&(*id, tags.clone()));
            }
        }
    }

    fn accumulate_output(state: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() {
            return;
        }
        let ids = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let tags = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for idx in 0..batch.num_rows() {
            let key = (ids.value(idx), tags.value(idx));
            let entry = state.entry(key).or_insert(0);
            *entry += batch.weights[idx];
            if *entry == 0 {
                state.remove(&key);
            }
        }
    }

    fn batch_reference(input: &BTreeMap<(i64, Vec<i64>), i64>) -> BTreeMap<(i64, i64), i64> {
        let mut out = BTreeMap::new();
        for ((id, tags), weight) in input {
            for tag in tags {
                let entry = out.entry((*id, *tag)).or_insert(0);
                *entry += *weight;
                if *entry == 0 {
                    out.remove(&(*id, *tag));
                }
            }
        }
        out
    }

    #[test]
    fn oracle_lateral_unnest_matches_batch() {
        let op = LateralOp::new(docs_schema(), LateralFunc::Unnest { col: 1 }).unwrap();
        let epochs = vec![
            vec![(1, vec![10, 20], 1), (2, vec![30], 1)],
            vec![(1, vec![10, 20], -1), (1, vec![40], 1)],
            vec![(2, vec![30], -1), (3, vec![], 1), (4, vec![50, 60], 2)],
        ];

        let mut input_state = BTreeMap::new();
        let mut incremental_state = BTreeMap::new();
        for epoch in &epochs {
            accumulate_input(&mut input_state, epoch);
            let delta = op.apply(make_epoch(epoch)).unwrap();
            accumulate_output(&mut incremental_state, &delta);
            assert_eq!(
                incremental_state,
                batch_reference(&input_state),
                "incremental lateral output must match batch expansion after epoch {epoch:?}"
            );
        }
    }
}
