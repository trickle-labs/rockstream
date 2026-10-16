//! Production operator pipelines used by the runtime, compared as typed signed bags (#123 R1, R4, R5).
use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rockstream_ops::{
    aggregate::{AggregateOp, DecimalAggregateFormatOp},
    factorized::{FactorizedAggregateKind, FactorizedJoinAggregateOp},
    join::JoinOp,
    live_exec::{JoinKind, JoinPipeline, Stage, StatefulPipeline, Utf8KeyPacker},
    minmax::{MinMaxKind, MinMaxOp},
    op::Operator,
    time_window::{HopWindowOp, SessionWindowOp, TumbleWindowOp},
    ArrowZSet,
};
use rockstream_plan::LateDataPolicy;
use rockstream_test_support::typed_output::{assert_weighted_eq, int64_batch};
use rockstream_types::ids::OperatorId;
use std::{collections::BTreeMap, sync::Arc};

fn input(rows: &[(&[Option<i64>], i64)]) -> ArrowZSet {
    let (data, weights) = int64_batch(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        rows,
    );
    ArrowZSet::new(data, weights)
}

fn assert_i64(out: &ArrowZSet, fields: &[(&str, bool)], rows: &[(&[Option<i64>], i64)]) {
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, nullable)| Field::new(*name, DataType::Int64, *nullable))
            .collect::<Vec<_>>(),
    ));
    let (data, weights) = int64_batch(schema, rows);
    assert_weighted_eq(&out.data, &out.weights, &data, &weights);
}

fn assert_aggregate(
    out: &ArrowZSet,
    keys: ArrayRef,
    names: [&str; 4],
    rows: &[(i64, i64, f64, i64)],
) {
    let fields = vec![
        Field::new(names[0], keys.data_type().clone(), false),
        Field::new(names[1], DataType::Int64, false),
        Field::new(names[2], DataType::Int64, false),
        Field::new(names[3], DataType::Float64, false),
    ];
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            keys,
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap();
    assert_weighted_eq(
        &out.data,
        &out.weights,
        &data,
        &rows.iter().map(|r| r.3).collect::<Vec<_>>(),
    );
}

#[test]
fn int8_sum_count_avg_pipeline_exact() {
    let op = Arc::new(AggregateOp::new(OperatorId(1)));
    assert_eq!(op.name(), "AggregateOp");
    let pipeline = StatefulPipeline::new().push(Stage::Aggregate(op));
    let out = pipeline
        .process(input(&[
            (&[Some(1), Some(100)], 1),
            (&[Some(1), Some(50)], 1),
            (&[Some(1), Some(75)], 1),
            (&[Some(2), Some(20)], 1),
        ]))
        .unwrap();
    // Independent batch arithmetic supplies expected values, never the executed result.
    let mut expected = BTreeMap::<i64, (i64, i64)>::new();
    for (key, value) in [(1, 100), (1, 50), (1, 75), (2, 20)] {
        let (sum, count) = expected.entry(key).or_default();
        *sum += value;
        *count += 1;
    }
    let rows = expected
        .values()
        .map(|(sum, count)| (*sum, *count, *sum as f64 / *count as f64, 1))
        .collect::<Vec<_>>();
    assert_aggregate(
        &out,
        Arc::new(Int64Array::from(vec![1, 2])),
        ["k", "sum_v", "count", "avg_v"],
        &rows,
    );
    let out = pipeline
        .process(input(&[(&[Some(1), Some(50)], -1)]))
        .unwrap();
    assert_aggregate(
        &out,
        Arc::new(Int64Array::from(vec![1, 1])),
        ["k", "sum_v", "count", "avg_v"],
        &[(225, 3, 75.0, -1), (175, 2, 87.5, 1)],
    );
}

#[test]
fn text_sum_count_avg_pipeline_exact() {
    let packer = Arc::new(Utf8KeyPacker::new());
    let pipeline = StatefulPipeline::new()
        .push(Stage::Utf8KeyPack(packer.clone(), OperatorId(2)))
        .push(Stage::Aggregate(Arc::new(AggregateOp::new(OperatorId(3)))))
        .push(Stage::Utf8KeyUnpack(packer, OperatorId(2)));
    let text_input = |keys: Vec<&str>, vals: Vec<i64>, weights| {
        ArrowZSet::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Utf8, false),
                    Field::new("v", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(StringArray::from(keys)),
                    Arc::new(Int64Array::from(vals)),
                ],
            )
            .unwrap(),
            weights,
        )
    };
    let out = pipeline
        .process(text_input(
            vec!["alice", "bob", "alice"],
            vec![100, 200, 50],
            vec![1, 1, 1],
        ))
        .unwrap();
    assert_aggregate(
        &out,
        Arc::new(StringArray::from(vec!["alice", "bob"])),
        ["k", "r0", "r1", "r2"],
        &[(150, 2, 75.0, 1), (200, 1, 200.0, 1)],
    );
    let out = pipeline
        .process(text_input(vec!["alice"], vec![50], vec![-1]))
        .unwrap();
    assert_aggregate(
        &out,
        Arc::new(StringArray::from(vec!["alice", "alice"])),
        ["k", "r0", "r1", "r2"],
        &[(150, 2, 75.0, -1), (100, 1, 100.0, 1)],
    );
}

#[test]
fn scaled_decimal_avg_text_pipeline_exact() {
    let format = Arc::new(DecimalAggregateFormatOp::new(2, true));
    assert_eq!(format.name(), "DecimalAggregateFormatOp");
    let pipeline = StatefulPipeline::new()
        .push(Stage::Aggregate(Arc::new(AggregateOp::new(OperatorId(4)))))
        .push(Stage::Stateless(format));
    let assert_decimal = |out: ArrowZSet, keys, values: Vec<&str>, weights: Vec<i64>| {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("agg", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap();
        assert_weighted_eq(&out.data, &out.weights, &data, &weights);
    };
    assert_decimal(
        pipeline
            .process(input(&[
                (&[Some(1), Some(10050)], 1),
                (&[Some(1), Some(19950)], 1),
                (&[Some(2), Some(5000)], 1),
            ]))
            .unwrap(),
        vec![1, 2],
        vec!["150", "50"],
        vec![1, 1],
    );
    assert_decimal(
        pipeline
            .process(input(&[(&[Some(1), Some(19950)], -1)]))
            .unwrap(),
        vec![1, 1],
        vec!["150", "100.5"],
        vec![-1, 1],
    );
}

fn qualify_minmax(kind: MinMaxKind, old: i64, removed: i64, new: i64) {
    let op = Arc::new(MinMaxOp::new(OperatorId(5), kind));
    assert_eq!(
        op.name(),
        if kind == MinMaxKind::Min {
            "MinOp"
        } else {
            "MaxOp"
        }
    );
    let pipeline = StatefulPipeline::new().push(Stage::MinMax(op, OperatorId(5)));
    let out = pipeline
        .process(input(&[
            (&[Some(1), Some(50)], 1),
            (&[Some(1), Some(75)], 1),
            (&[Some(1), Some(100)], 1),
        ]))
        .unwrap();
    assert_i64(
        &out,
        &[("k", false), ("extremum_v", false)],
        &[(&[Some(1), Some(old)], 1)],
    );
    let out = pipeline
        .process(input(&[(&[Some(1), Some(removed)], -1)]))
        .unwrap();
    assert_i64(
        &out,
        &[("k", false), ("extremum_v", false)],
        &[(&[Some(1), Some(old)], -1), (&[Some(1), Some(new)], 1)],
    );
}

#[test]
fn int8_min_pipeline_exact() {
    qualify_minmax(MinMaxKind::Min, 50, 50, 75);
}
#[test]
fn int8_max_pipeline_exact() {
    qualify_minmax(MinMaxKind::Max, 100, 100, 75);
}

#[test]
fn classic_join_pipeline_exact() {
    let pipeline = JoinPipeline::new(
        vec![],
        vec![],
        JoinKind::Inner(Arc::new(JoinOp::new(OperatorId(6), vec![0], vec![0]))),
        vec![],
    );
    assert_eq!(pipeline.strategy(), "classic");
    let out = pipeline
        .process(
            input(&[(&[Some(1), Some(10)], 1), (&[Some(2), Some(20)], 1)]),
            input(&[(&[Some(1), Some(30)], 1), (&[Some(1), Some(40)], 1)]),
        )
        .unwrap();
    let fields = [
        ("l_0", false),
        ("l_1", false),
        ("r_0", false),
        ("r_1", false),
    ];
    assert_i64(
        &out,
        &fields,
        &[
            (&[Some(1), Some(10), Some(1), Some(30)], 1),
            (&[Some(1), Some(10), Some(1), Some(40)], 1),
        ],
    );
    let out = pipeline
        .process(input(&[(&[Some(1), Some(10)], -1)]), input(&[]))
        .unwrap();
    assert_i64(
        &out,
        &fields,
        &[
            (&[Some(1), Some(10), Some(1), Some(30)], -1),
            (&[Some(1), Some(10), Some(1), Some(40)], -1),
        ],
    );
}

#[test]
fn factorized_join_sum_pipeline_exact() {
    let op = Arc::new(FactorizedJoinAggregateOp::new(
        OperatorId(7),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    ));
    let pipeline = JoinPipeline::new(vec![], vec![], JoinKind::Factorized(op), vec![]);
    assert_eq!(pipeline.strategy(), "factorized");
    let out = pipeline
        .process(
            input(&[(&[Some(1), Some(10)], 1)]),
            input(&[(&[Some(1), Some(30)], 1), (&[Some(1), Some(40)], 1)]),
        )
        .unwrap();
    let fields = [("group", true), ("aggregate", false)];
    assert_i64(&out, &fields, &[(&[Some(1), Some(70)], 1)]);
    let out = pipeline
        .process(input(&[]), input(&[(&[Some(1), Some(40)], -1)]))
        .unwrap();
    assert_i64(
        &out,
        &fields,
        &[(&[Some(1), Some(70)], -1), (&[Some(1), Some(30)], 1)],
    );
}

#[test]
fn tumble_window_pipeline_exact() {
    let op = Arc::new(TumbleWindowOp::new(
        input(&[]).schema(),
        0,
        60000,
        LateDataPolicy::Drop,
    ));
    assert_eq!(op.name(), "TumbleWindowOp");
    let pipeline = StatefulPipeline::new().push(Stage::TumbleWindow(op, OperatorId(8)));
    let fields = [("window_id", false), ("k", false), ("v", false)];
    let window_start = (160000 / 60000) * 60000;
    assert_i64(
        &pipeline
            .process(input(&[(&[Some(160000), Some(7)], 1)]))
            .unwrap(),
        &fields,
        &[(&[Some(window_start), Some(160000), Some(7)], 1)],
    );
    assert_i64(
        &pipeline
            .process(input(&[(&[Some(160000), Some(7)], -1)]))
            .unwrap(),
        &fields,
        &[(&[Some(window_start), Some(160000), Some(7)], -1)],
    );
}

#[test]
fn hop_window_pipeline_exact() {
    let op = Arc::new(HopWindowOp::new(
        input(&[]).schema(),
        0,
        300000,
        60000,
        LateDataPolicy::Drop,
    ));
    assert_eq!(op.name(), "HopWindowOp");
    let pipeline = StatefulPipeline::new().push(Stage::HopWindow(op, OperatorId(9)));
    let fields = [("window_id", false), ("k", false), ("v", false)];
    let rows = [
        (&[Some(-120000), Some(150000), Some(7)][..], 1),
        (&[Some(-60000), Some(150000), Some(7)][..], 1),
        (&[Some(0), Some(150000), Some(7)][..], 1),
        (&[Some(60000), Some(150000), Some(7)][..], 1),
        (&[Some(120000), Some(150000), Some(7)][..], 1),
    ];
    assert_i64(
        &pipeline
            .process(input(&[(&[Some(150000), Some(7)], 1)]))
            .unwrap(),
        &fields,
        &rows,
    );
    let retractions = rows.iter().map(|(r, w)| (*r, -*w)).collect::<Vec<_>>();
    assert_i64(
        &pipeline
            .process(input(&[(&[Some(150000), Some(7)], -1)]))
            .unwrap(),
        &fields,
        &retractions,
    );
}

#[test]
fn session_window_pipeline_exact() {
    let op = Arc::new(SessionWindowOp::new(
        input(&[]).schema(),
        0,
        1800000,
        LateDataPolicy::Drop,
    ));
    assert_eq!(op.name(), "SessionWindowOp");
    let pipeline = StatefulPipeline::new().push(Stage::SessionWindow(op, OperatorId(10)));
    let fields = [
        ("session_start", false),
        ("session_end", false),
        ("k", false),
        ("v", false),
    ];
    let out = pipeline
        .process(input(&[
            (&[Some(1000), Some(7)], 1),
            (&[Some(5000), Some(7)], 1),
            (&[Some(10000), Some(7)], 1),
            (&[Some(2000000), Some(7)], 1),
        ]))
        .unwrap();
    assert_i64(
        &out,
        &fields,
        &[
            (&[Some(1000), Some(10000), Some(1000), Some(7)], 1),
            (&[Some(1000), Some(10000), Some(5000), Some(7)], 1),
            (&[Some(1000), Some(10000), Some(10000), Some(7)], 1),
            (&[Some(2000000), Some(2000000), Some(2000000), Some(7)], 1),
        ],
    );
    let out = pipeline
        .process(input(&[(&[Some(10000), Some(7)], -1)]))
        .unwrap();
    assert_i64(
        &out,
        &fields,
        &[
            (&[Some(1000), Some(10000), Some(1000), Some(7)], -1),
            (&[Some(1000), Some(10000), Some(5000), Some(7)], -1),
            (&[Some(1000), Some(10000), Some(10000), Some(7)], -1),
            (&[Some(1000), Some(5000), Some(1000), Some(7)], 1),
            (&[Some(1000), Some(5000), Some(5000), Some(7)], 1),
        ],
    );
}
