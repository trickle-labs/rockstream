//! Issue #402 Verification 4 & R5:
//! Stateless Fusion (`Filter -> Project -> Map`), Once-Per-Batch `BatchKeyCapsule`
//! Reuse, Single-Owner Zero-Mutex Execution, and Oracle Bit-Identical Equivalence.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::expr::lit;
use rockstream_ops::project::NamedExpr;
use rockstream_ops::sink::materialize_view_state;
use rockstream_ops::{
    compile_plan_with_strategy, read_view_output, AggregateOp, AggregateStateCore, ArrowZSet,
    ColumnValue, FilterOp, FusedStatelessKernel, JoinOp, JoinStateCore, MapOp, Operator,
    OwnedJoinKind, OwnedJoinPipeline, OwnedStage, OwnedStatefulPipeline, ProjectOp, Stage,
    StatelessStep,
};
use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr, JoinSemantics, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_test_support::external_harness::MultisetOracle;
use rockstream_types::config::JoinStrategy;
use rockstream_types::ids::OperatorId;
use rockstream_types::measure_batch_key_capsule_encodings;

fn make_3col_batch(rows: &[(i64, i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(batch, rows.iter().map(|r| r.3).collect())
}

fn make_2col_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(batch, rows.iter().map(|r| r.2).collect())
}

fn accumulate_2col(zset: &ArrowZSet, acc: &mut BTreeMap<(i64, i64), i64>) {
    if zset.is_empty() {
        return;
    }
    let c0 = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c1 = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for (i, &weight) in zset.weights.iter().enumerate() {
        let key = (c0.value(i), c1.value(i));
        let w = acc.entry(key).or_insert(0);
        *w += weight;
        if *w == 0 {
            acc.remove(&key);
        }
    }
}

fn accumulate_1col(zset: &ArrowZSet, acc: &mut BTreeMap<i64, i64>) {
    if zset.is_empty() {
        return;
    }
    assert_eq!(zset.data.num_columns(), 1);
    let c0 = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for (i, &weight) in zset.weights.iter().enumerate() {
        let key = c0.value(i);
        let w = acc.entry(key).or_insert(0);
        *w += weight;
        if *w == 0 {
            acc.remove(&key);
        }
    }
}

#[tokio::test]
async fn compiled_filter_project_map_fuses_and_matches_unfused_bit_identically() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("fusion-stateless", store)
            .build()
            .await
            .unwrap(),
    );

    let src_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let mut schemas = HashMap::new();
    schemas.insert("src".to_string(), src_schema);

    let pred1 = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column(1)),
        right: Box::new(lit(5)),
    };
    let pred2 = Expr::BinaryOp {
        op: BinaryOp::Lt,
        left: Box::new(Expr::Column(2)),
        right: Box::new(lit(100)),
    };
    let proj_cols = vec![
        Expr::Column(0),
        Expr::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(Expr::Column(1)),
            right: Box::new(Expr::Column(2)),
        },
    ];
    let map_expr = Expr::BinaryOp {
        op: BinaryOp::Mul,
        left: Box::new(Expr::Column(1)),
        right: Box::new(lit(2)),
    };

    let plan = PlanNode::ViewSink {
        view_name: "v_fused_stateless".to_string(),
        pk: vec![0],
        child: Box::new(PlanNode::Map {
            func: map_expr.clone(),
            input: Box::new(PlanNode::Project {
                columns: proj_cols.clone(),
                input: Box::new(PlanNode::Filter {
                    predicate: pred2.clone(),
                    input: Box::new(PlanNode::Filter {
                        predicate: pred1.clone(),
                        input: Box::new(PlanNode::Source {
                            name: "src".to_string(),
                        }),
                    }),
                }),
            }),
        }),
    };

    let compiled =
        compile_plan_with_strategy(&plan, db.clone(), &schemas, JoinStrategy::Auto).unwrap();

    // All 4 adjacent stateless operators must collapse into 1 Stage::FusedStateless.
    assert_eq!(compiled.pipeline.stages().len(), 1);
    let kernel = match &compiled.pipeline.stages()[0] {
        Stage::FusedStateless(k) => k,
        other => panic!("expected Stage::FusedStateless, got {}", other.kind_name()),
    };
    assert_eq!(kernel.fused_stage_count(), 4);
    assert_eq!(kernel.physical_op_count(), 1);

    // Unfused reference operators.
    let f1 = FilterOp::new(pred1);
    let f2 = FilterOp::new(pred2);
    let p = ProjectOp::new(vec![
        NamedExpr::new("col0", proj_cols[0].clone()),
        NamedExpr::new("col1", proj_cols[1].clone()),
    ]);
    let m = MapOp::new(map_expr, "value");

    let epochs = vec![
        make_3col_batch(&[
            (1, 10, 20, 1),
            (2, 3, 50, 1),   // filtered out by pred1 (a <= 5)
            (3, 15, 120, 1), // filtered out by pred2 (b >= 100)
            (4, 20, 30, 1),
        ]),
        make_3col_batch(&[
            (1, 10, 20, -1), // delete
            (1, 12, 25, 1),  // update
            (5, 6, 99, 1),   // boundary pass
        ]),
    ];

    let mut fused_acc = BTreeMap::new();
    let mut unfused_acc = BTreeMap::new();
    for batch in epochs {
        let fused_out = compiled.pipeline.process(batch.clone()).unwrap();
        let unfused_out = m
            .process_delta(
                p.process_delta(f2.process_delta(f1.process_delta(batch).unwrap()).unwrap())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(fused_out.schema(), unfused_out.schema());
        accumulate_1col(&fused_out, &mut fused_acc);
        accumulate_1col(&unfused_out, &mut unfused_acc);
    }

    assert_eq!(fused_acc, unfused_acc);
    assert_eq!(fused_acc, BTreeMap::from([(74, 1), (100, 1), (210, 1)]));
    assert_eq!(kernel.batches_processed(), 2);
    assert_eq!(kernel.intermediate_batches_elided(), 6); // 3 elided per batch × 2 batches
    drop(compiled);
    Arc::try_unwrap(db)
        .ok()
        .expect("unique db")
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn fused_filter_project_join_aggregate_view_sink_single_key_encode_and_oracle_parity() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("fusion-join-agg", store.clone())
            .build()
            .await
            .unwrap(),
    );

    let left_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let right_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let mut schemas = HashMap::new();
    schemas.insert("left_t".to_string(), left_schema);
    schemas.insert("right_t".to_string(), right_schema);

    // Filter(a > 0) -> Project(k, a + b) on left, Filter(v > 0) on right ->
    // InnerJoin(k=k) -> Aggregate(GROUP BY l_k, SUM(l_val)) -> ViewSink.
    let left_plan = PlanNode::Project {
        columns: vec![
            Expr::Column(0),
            Expr::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(Expr::Column(1)),
                right: Box::new(Expr::Column(2)),
            },
        ],
        input: Box::new(PlanNode::Filter {
            predicate: Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(0)),
            },
            input: Box::new(PlanNode::Source {
                name: "left_t".to_string(),
            }),
        }),
    };
    let right_plan = PlanNode::Filter {
        predicate: Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(0)),
        },
        input: Box::new(PlanNode::Source {
            name: "right_t".to_string(),
        }),
    };
    let plan = PlanNode::ViewSink {
        view_name: "v_join_agg".to_string(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
            input: Box::new(PlanNode::InnerJoin {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
                left_keys: vec![0],
                right_keys: vec![0],
                left_arr_id: OperatorId(801),
                right_arr_id: OperatorId(802),
                semantics: JoinSemantics::default(),
            }),
        }),
    };

    let compiled =
        compile_plan_with_strategy(&plan, db.clone(), &schemas, JoinStrategy::Classic).unwrap();
    let join_compiled = compiled.join.as_ref().expect("compiled as JoinPipeline");

    // Left pre-stages (Filter + Project) must be fused into 1 FusedStateless stage with prepack!
    assert_eq!(join_compiled.pipeline.left_pre().len(), 1);
    match &join_compiled.pipeline.left_pre()[0] {
        Stage::FusedStateless(k) => {
            assert_eq!(k.fused_stage_count(), 2);
            assert_eq!(k.physical_op_count(), 1);
            assert_eq!(k.prepack_key_cols(), Some(&[0usize][..]));
        }
        other => panic!(
            "expected FusedStateless left_pre, got {}",
            other.kind_name()
        ),
    }
    assert_eq!(join_compiled.pipeline.right_pre().len(), 1);
    match &join_compiled.pipeline.right_pre()[0] {
        Stage::FusedStateless(k) => {
            assert_eq!(k.prepack_key_cols(), Some(&[0usize][..]));
        }
        other => panic!(
            "expected FusedStateless right_pre, got {}",
            other.kind_name()
        ),
    }

    // Build an equivalent single-owner zero-Mutex OwnedJoinPipeline.
    let mut owned_pipeline = OwnedJoinPipeline::new(
        vec![OwnedStage::FusedStateless(
            FusedStatelessKernel::compile(vec![
                StatelessStep::Filter(Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(0)),
                }),
                StatelessStep::Project(vec![
                    NamedExpr::new("col0", Expr::Column(0)),
                    NamedExpr::new(
                        "col1",
                        Expr::BinaryOp {
                            op: BinaryOp::Add,
                            left: Box::new(Expr::Column(1)),
                            right: Box::new(Expr::Column(2)),
                        },
                    ),
                ]),
            ])
            .with_prepack_key_cols(vec![0]),
        )],
        vec![OwnedStage::FusedStateless(
            FusedStatelessKernel::compile(vec![StatelessStep::Filter(Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(0)),
            })])
            .with_prepack_key_cols(vec![0]),
        )],
        OwnedJoinKind::Inner(JoinStateCore::with_schema(
            OperatorId(811),
            vec![0],
            vec![0],
            2,
            2,
        )),
        vec![
            OwnedStage::FusedStateless(
                FusedStatelessKernel::compile(vec![StatelessStep::Project(vec![
                    NamedExpr::new("k", Expr::Column(0)),
                    NamedExpr::new("v", Expr::Column(1)),
                ])])
                .with_prepack_key_cols(vec![0]),
            ),
            OwnedStage::Aggregate(AggregateStateCore::new(OperatorId(812))),
            OwnedStage::FusedStateless(FusedStatelessKernel::compile(vec![
                StatelessStep::Project(vec![
                    NamedExpr::new("k", Expr::Column(0)),
                    NamedExpr::new("agg", Expr::Column(1)),
                ]),
            ])),
        ],
    );

    // Unfused reference pipeline.
    let unfused_left_f = FilterOp::new(Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column(1)),
        right: Box::new(lit(0)),
    });
    let unfused_left_p = ProjectOp::new(vec![
        NamedExpr::new("k", Expr::Column(0)),
        NamedExpr::new(
            "v",
            Expr::BinaryOp {
                op: BinaryOp::Add,
                left: Box::new(Expr::Column(1)),
                right: Box::new(Expr::Column(2)),
            },
        ),
    ]);
    let unfused_right_f = FilterOp::new(Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column(1)),
        right: Box::new(lit(0)),
    });
    let unfused_join = JoinOp::new(OperatorId(821), vec![0], vec![0]);
    let unfused_agg = AggregateOp::new(OperatorId(822));
    let unfused_post = ProjectOp::new(vec![
        NamedExpr::new("k", Expr::Column(0)),
        NamedExpr::new("agg", Expr::Column(1)),
    ]);

    let epochs = vec![
        (
            make_3col_batch(&[(1, 10, 5, 1), (1, -5, 99, 1), (2, 7, 3, 1)]),
            make_2col_batch(&[(1, 100, 1), (1, 200, 1), (2, 50, 1), (2, -1, 1)]),
        ),
        (
            make_3col_batch(&[(1, 10, 5, -1), (1, 20, 10, 1)]),
            make_2col_batch(&[(1, 200, -1), (3, 75, 1)]),
        ),
        (
            make_3col_batch(&[(3, 4, 6, 1), (2, 7, 3, -1)]),
            make_2col_batch(&[]),
        ),
    ];

    let mut fused_acc = BTreeMap::new();
    let mut owned_acc = BTreeMap::new();
    let mut unfused_acc = BTreeMap::new();

    for (epoch_idx, (left_b, right_b)) in epochs.into_iter().enumerate() {
        // Measure BatchKeyCapsule encodings on the compiled fused join pipeline:
        // Left prepack encodes left key ONCE (if non-empty after filter),
        // Right prepack encodes right key ONCE (if non-empty after filter),
        // JoinOp reuses BOTH capsules with 0 additional encodings, and propagates
        // the join-key capsule through post-Project into AggregateOp!
        let (fused_out, encodings) = measure_batch_key_capsule_encodings(|| {
            join_compiled
                .pipeline
                .process(left_b.clone(), right_b.clone())
        });
        let fused_out = fused_out.unwrap();
        let expected_max_enc = u64::from(!left_b.is_empty()) + u64::from(!right_b.is_empty());
        assert!(
            encodings <= expected_max_enc,
            "expected at most {expected_max_enc} key capsule encodings per batch, got {encodings}"
        );

        let owned_out = owned_pipeline
            .process(left_b.clone(), right_b.clone())
            .unwrap();

        let ul = unfused_left_p
            .process_delta(unfused_left_f.process_delta(left_b).unwrap())
            .unwrap();
        let ur = unfused_right_f.process_delta(right_b).unwrap();
        let uj = unfused_join.process_epoch(ul, ur).unwrap();
        let ua = unfused_agg.process_delta(uj).unwrap();
        let unfused_out = unfused_post.process_delta(ua).unwrap();

        compiled
            .sink
            .write_epoch(&fused_out, epoch_idx as u64 + 1)
            .await
            .unwrap();
        accumulate_2col(&fused_out, &mut fused_acc);
        accumulate_2col(&owned_out, &mut owned_acc);
        accumulate_2col(&unfused_out, &mut unfused_acc);
    }

    // Ground truth via direct calculation:
    // Epoch 1: L=[(1,15), (2,10)], R=[(1,100),(1,200),(2,50)] -> k=1 sum=30, k=2 sum=10
    // Epoch 2: L=[(1,30), (2,10)], R=[(1,100),(2,50),(3,75)] -> k=1 sum=30, k=2 sum=10
    // Epoch 3: L=[(1,30), (3,10)], R=[(1,100),(2,50),(3,75)] -> k=1 sum=30, k=3 sum=10
    let mut oracle = MultisetOracle::new();
    oracle.ingest_aggregate_event(1, 30, 1);
    oracle.ingest_aggregate_event(3, 10, 1);
    let expected = BTreeMap::from([((1, 30), 1), ((3, 10), 1)]);

    assert_eq!(fused_acc, expected);
    assert_eq!(owned_acc, expected);
    assert_eq!(unfused_acc, expected);

    // Verify ViewSink storage holds exact rows.
    let sink_changes = read_view_output(&db, compiled.sink_op_id, 2).await.unwrap();
    let materialized = materialize_view_state(sink_changes, &compiled.pk);
    let actual_sink: BTreeMap<i64, i64> = materialized
        .into_values()
        .map(|(row, w)| {
            assert_eq!(w, 1);
            match (&row[0], &row[1]) {
                (ColumnValue::Int64(k), ColumnValue::Int64(s)) => (*k, *s),
                other => panic!("unexpected view sink row: {other:?}"),
            }
        })
        .collect();
    assert_eq!(actual_sink, BTreeMap::from([(1, 30), (3, 10)]));
    assert_eq!(
        oracle.expected_aggregates(),
        vec![(1, 30, 1, 30.0), (3, 10, 1, 10.0)]
    );

    // Persist owned pipeline state cleanly.
    owned_pipeline.persist(&db).await.unwrap();
    db.flush().await.unwrap();
    drop(compiled);
    Arc::try_unwrap(db)
        .ok()
        .expect("unique db")
        .close()
        .await
        .unwrap();
}

#[test]
fn single_owner_zero_mutex_pipeline_profile_and_exactness() {
    // Measure tight-loop throughput of single-owner (&mut self) OwnedStatefulPipeline
    // vs shared-Mutex StatefulPipeline over 2,000 micro-batches with zero mutex locking
    // on the owned path.
    let mut owned = OwnedStatefulPipeline::new()
        .push(OwnedStage::FusedStateless(
            FusedStatelessKernel::compile(vec![
                StatelessStep::Filter(Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(0)),
                }),
                StatelessStep::Project(vec![
                    NamedExpr::new("k", Expr::Column(0)),
                    NamedExpr::new("v", Expr::Column(1)),
                ]),
            ])
            .with_prepack_key_cols(vec![0]),
        ))
        .push(OwnedStage::Aggregate(AggregateStateCore::new(OperatorId(
            999,
        ))));

    let batch = make_2col_batch(&[(1, 5, 1), (2, 10, 1), (3, -2, 1), (4, 15, 1)]);
    let started = Instant::now();
    let mut last_out = ArrowZSet::empty(batch.schema());
    for _ in 0..1_000 {
        last_out = owned.process(batch.clone()).unwrap();
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed.as_secs() < 10,
        "1,000 fused owned batches should finish rapidly (took {elapsed:?})"
    );
    // Final iteration updates k=1 -> 5000, k=2 -> 10000, k=4 -> 15000.
    let ks = last_out
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = last_out
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut final_pos = BTreeMap::new();
    for (i, &weight) in last_out.weights.iter().enumerate() {
        if weight > 0 {
            final_pos.insert(ks.value(i), sums.value(i));
        }
    }
    assert_eq!(
        final_pos,
        BTreeMap::from([(1, 5_000), (2, 10_000), (4, 15_000)])
    );
}
