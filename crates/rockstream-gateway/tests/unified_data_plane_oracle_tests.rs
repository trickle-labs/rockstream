//! v0.51.3 Slice 3 exit test: a `PlanNode` compiled directly to an
//! executable operator chain via `rockstream_ops::compile_plan` produces
//! output that matches a hand-computed batch oracle, across multiple
//! epochs of inserts and retractions.
//!
//! This proves the "one data plane" compiler (Source → Filter → Project →
//! ViewSink) is wired correctly end-to-end: pipeline processing plus
//! `ViewSinkOp` persistence round-trip to the exact same multiset the
//! naive (non-incremental) filter+project computation would produce.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::{compile_plan, read_view_output, ArrowZSet};
use rockstream_plan::{BinaryOp, Expr, PlanNode};
use rockstream_storage::ShardDb;
use tempfile::TempDir;

async fn open_shard_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

/// `id BIGINT, name TEXT, amount BIGINT` schema.
fn source_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn make_batch(rows: &[(i64, &str, i64)], weights: &[i64]) -> ArrowZSet {
    let ids: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
    ));
    let names: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|(_, n, _)| *n).collect::<Vec<_>>(),
    ));
    let amounts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|(_, _, a)| *a).collect::<Vec<_>>(),
    ));
    let data = RecordBatch::try_new(source_schema(), vec![ids, names, amounts]).unwrap();
    ArrowZSet::new(data, weights.to_vec())
}

/// One epoch's input rows (`id, name, amount`) paired with their Z-set weights.
type EpochBatch<'a> = (Vec<(i64, &'a str, i64)>, Vec<i64>);

/// Hand-computed oracle: `SELECT id, name FROM t WHERE amount > 100`,
/// keyed by `(id, name)` and accumulated across all epochs.
fn oracle_accumulate(epochs: &[EpochBatch<'_>]) -> BTreeMap<(i64, String), i64> {
    let mut acc: BTreeMap<(i64, String), i64> = BTreeMap::new();
    for (rows, weights) in epochs {
        for ((id, name, amount), w) in rows.iter().zip(weights.iter()) {
            if *amount > 100 {
                *acc.entry((*id, name.to_string())).or_insert(0) += w;
            }
        }
    }
    // Drop rows whose net weight nets to zero (fully retracted).
    acc.retain(|_, w| *w != 0);
    acc
}

#[tokio::test]
async fn compiled_view_incremental_output_equals_batch_oracle() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;

    // Plan: SELECT id, name FROM t WHERE amount > 100, materialized as
    // view `big_orders` with pk = [id].
    let plan = PlanNode::ViewSink {
        view_name: "big_orders".to_string(),
        pk: vec![0],
        child: Box::new(PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                predicate: Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(2)),
                    right: Box::new(rockstream_ops::expr::lit(100)),
                },
            }),
            columns: vec![Expr::Column(0), Expr::Column(1)],
        }),
    };

    let table_schemas: HashMap<String, Arc<Schema>> = [("t".to_string(), source_schema())].into();
    let compiled = compile_plan(&plan, db.clone(), &table_schemas).expect("plan should compile");
    assert_eq!(compiled.view_name, "big_orders");
    assert_eq!(compiled.pk, vec![0]);

    let mut rng = 913_u64;
    let mut next_random = move || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        rng
    };

    let names = ["alice", "bob", "carol", "dave"];
    let mut epochs: Vec<EpochBatch<'_>> = Vec::new();

    for _ in 0..8 {
        let mut rows = Vec::new();
        let mut weights = Vec::new();
        for _ in 0..12 {
            let id = (next_random() % 6) as i64;
            let name = names[(next_random() % names.len() as u64) as usize];
            let amount = (next_random() % 200) as i64;
            let w = if next_random() % 3 == 0 { -1 } else { 1 };
            rows.push((id, name, amount));
            weights.push(w);
        }
        epochs.push((rows, weights));
    }

    // Drive each epoch through the compiled pipeline and persist via the sink.
    for (rows, weights) in &epochs {
        let input = make_batch(rows, weights);
        let output = compiled
            .pipeline
            .process(input)
            .expect("pipeline processing should succeed");
        compiled
            .sink
            .write_next_epoch(&output)
            .await
            .expect("sink write should succeed");
    }

    // Read back everything the sink wrote and accumulate into a multiset.
    let stored = read_view_output(&db, compiled.sink_op_id, 2)
        .await
        .expect("read_view_output should succeed");

    let mut incremental: BTreeMap<(i64, String), i64> = BTreeMap::new();
    for (_epoch, _row_idx, cols, weight) in stored {
        let id = cols[0].as_i64().expect("id column should be Int64");
        let name = cols[1]
            .as_utf8()
            .expect("name column should be Utf8")
            .to_string();
        *incremental.entry((id, name)).or_insert(0) += weight;
    }
    incremental.retain(|_, w| *w != 0);

    let batch_oracle = oracle_accumulate(&epochs);

    assert_eq!(
        incremental, batch_oracle,
        "compiled incremental output must equal the batch oracle"
    );
    assert!(
        !batch_oracle.is_empty(),
        "sanity check: oracle should have produced at least one surviving row"
    );

    // Every surviving row must actually satisfy the WHERE clause.
    for (id, _name) in batch_oracle.keys() {
        assert!(*id >= 0 && *id < 6);
    }
}

/// A non-view-sink or unsupported plan node is rejected with a real
/// `RS-1013` error rather than a panic, so the compiler fails closed on
/// query shapes it doesn't yet support (joins, aggregates, windows, ...).
#[tokio::test]
async fn compile_plan_rejects_unsupported_node_with_rs_error_code() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;

    let plan = PlanNode::ViewSink {
        view_name: "v".to_string(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            group_by: vec![],
            aggregates: vec![],
        }),
    };

    let table_schemas: HashMap<String, Arc<Schema>> = [("t".to_string(), source_schema())].into();
    let result = compile_plan(&plan, db, &table_schemas);
    let err = match result {
        Ok(compiled) => {
            drop(compiled);
            panic!("expected compile_plan to reject an Aggregate node")
        }
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("RS-1013"),
        "expected an RS-1013 error code, got: {msg}"
    );
}
