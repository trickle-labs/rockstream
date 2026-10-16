use std::{collections::BTreeMap, sync::Arc};

use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use object_store::ObjectStore;
use rockstream_ops::{
    topk::{load_topk_state, persist_topk_state, TopKOp, TOPK_BUFFER_LIMIT},
    zset::ArrowZSet,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_types::ids::OperatorId;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rank", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap(),
        rows.iter().map(|r| r.2).collect(),
    )
}

fn relation(zset: &ArrowZSet) -> BTreeMap<(i64, i64), i64> {
    assert_eq!(zset.data.schema(), schema());
    let ranks = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ids = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut result = BTreeMap::new();
    for (i, w) in zset.weights.iter().enumerate() {
        *result.entry((ranks.value(i), ids.value(i))).or_insert(0) += w;
    }
    result.retain(|_, w| *w != 0);
    result
}

// #114 R1-R3: identical production scenario runs on the named backend, including close/reopen.
pub async fn spilled_recovery(store: Arc<dyn ObjectStore>, path: &str) {
    assert_eq!(TOPK_BUFFER_LIMIT, 100_000);
    let db = Arc::new(ShardDb::builder(path, store.clone()).build().await.unwrap());
    let reference_db = Arc::new(
        ShardDb::builder(format!("{path}-reference"), store.clone())
            .build()
            .await
            .unwrap(),
    );
    let op = TopKOp::new(schema(), 2, 0, vec![]).with_db(db.clone());
    let reference = TopKOp::new(schema(), 2, 0, vec![]).with_db(reference_db.clone());
    let rows = (1..=100001).map(|v| (v, v, 1)).collect::<Vec<_>>();
    let initial = relation(&op.process_epoch(input(&rows), 1).unwrap());
    assert_eq!(
        initial,
        BTreeMap::from([((100000, 100000), 1), ((100001, 100001), 1)])
    );
    assert_eq!(
        relation(&reference.process_epoch(input(&rows), 1).unwrap()),
        initial
    );
    assert_eq!(op.fill_level(), TOPK_BUFFER_LIMIT);
    let spills = db.scan_prefix(b"topk_spill:").await.unwrap();
    assert_eq!(
        spills.iter().map(|(_, v)| v.to_vec()).collect::<Vec<_>>(),
        vec![[
            100001_i64.to_be_bytes(),
            1_i64.to_be_bytes(),
            100001_i64.to_be_bytes(),
            100001_i64.to_be_bytes()
        ]
        .concat()]
    );
    persist_topk_state(&db, &op, OperatorId(114)).await.unwrap();
    db.flush().await.unwrap();
    drop(op);
    Arc::try_unwrap(db)
        .ok()
        .expect("operator released the original backend")
        .close()
        .await
        .unwrap();

    let reopened = Arc::new(ShardDb::builder(path, store).build().await.unwrap());
    assert_eq!(reopened.scan_prefix(b"topk_spill:").await.unwrap(), spills);
    let recovered = load_topk_state(&reopened, schema(), 2, 0, vec![], OperatorId(114))
        .await
        .unwrap()
        .with_db(reopened.clone());
    assert_eq!(recovered.fill_level(), 100001);
    let mut output_state = initial;
    let steps = [
        (
            vec![(100001, 100001, -1)],
            BTreeMap::from([((100001, 100001), -1), ((99999, 99999), 1)]),
        ),
        (
            vec![(100000, 100000, 1)],
            BTreeMap::from([((99999, 99999), -1), ((100000, 100000), 1)]),
        ),
        (
            vec![(100000, 100000, -1)],
            BTreeMap::from([((100000, 100000), -1), ((99999, 99999), 1)]),
        ),
        (
            vec![(100001, 100001, 2)],
            BTreeMap::from([
                ((99999, 99999), -1),
                ((100000, 100000), -1),
                ((100001, 100001), 2),
            ]),
        ),
        (
            vec![(100001, 100001, -1)],
            BTreeMap::from([((100001, 100001), -1), ((100000, 100000), 1)]),
        ),
        (
            vec![(100001, 100001, -1)],
            BTreeMap::from([((100001, 100001), -1), ((99999, 99999), 1)]),
        ),
    ];
    for (i, (rows, expected)) in steps.into_iter().enumerate() {
        let actual = recovered.process_epoch(input(&rows), i as u64 + 2).unwrap();
        let uninterrupted = reference.process_epoch(input(&rows), i as u64 + 2).unwrap();
        assert_eq!(actual.data.schema(), uninterrupted.data.schema());
        assert_eq!(relation(&actual), expected);
        assert_eq!(relation(&uninterrupted), expected);
        for (row, w) in expected {
            *output_state.entry(row).or_insert(0) += w;
        }
        output_state.retain(|_, w| *w != 0);
        if i == 0 {
            assert_eq!(
                output_state,
                BTreeMap::from([((100000, 100000), 1), ((99999, 99999), 1)])
            );
        }
    }
    // A later checkpoint must remove cancelled rows rather than resurrecting them.
    persist_topk_state(&reopened, &recovered, OperatorId(114))
        .await
        .unwrap();
    let checkpoint = reopened
        .scan_prefix(&ShardKeyEncoder::topk_op_prefix(114))
        .await
        .unwrap();
    assert_eq!(
        checkpoint
            .iter()
            .map(|(_, v)| v.to_vec())
            .collect::<Vec<_>>(),
        (1_i64..=100000)
            .rev()
            .map(|v| {
                [
                    v.to_be_bytes(),
                    1_i64.to_be_bytes(),
                    v.to_be_bytes(),
                    v.to_be_bytes(),
                ]
                .concat()
            })
            .collect::<Vec<_>>()
    );
    drop(recovered);
    Arc::try_unwrap(reopened)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
    drop(reference);
    Arc::try_unwrap(reference_db)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
}
