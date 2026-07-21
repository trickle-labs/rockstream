//! LFS backend integration test for v0.4 operators.
//!
//! Proves: the Filter → Project → ViewSink pipeline correctly writes output
//! to a real SlateDB local-filesystem backend and the output survives a
//! close/reopen cycle.
//!
//! Test plan:
//! 1. Create a temporary directory and open a `ShardDb` on it.
//! 2. Build a pipeline: VecDeltaSource → Filter(b*2 > 10) → Project(a, b*2 AS c) → ViewSink.
//! 3. Push 3 epochs of delta batches.
//! 4. Verify that the correct rows were written to the `view_output` namespace.
//! 5. Close and reopen the `ShardDb`.
//! 6. Verify the data is still present after restart.

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_ops::filter::FilterOp;
use rockstream_ops::pipeline::LinearPipeline;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::sink::{read_view_output, ViewSinkOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{BinaryOp, Expr};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

fn lit(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

/// Open a fresh ShardDb backed by the local filesystem in `dir`.
async fn open_shard_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

/// Build Filter(b*2 > 10) → Project(a, b*2 AS c) pipeline.
fn make_pipeline() -> LinearPipeline {
    let predicate = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(2)),
        }),
        right: Box::new(lit(10)),
    };
    let project = ProjectOp::new(vec![
        NamedExpr::new("a", Expr::Column(0)),
        NamedExpr::new(
            "c",
            Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            },
        ),
    ]);
    LinearPipeline::new()
        .push(Arc::new(FilterOp::new(predicate)))
        .push(Arc::new(project))
}

/// Collect expected output from the input epochs using the batch reference.
fn batch_expected(epochs: &[Vec<(i64, i64)>]) -> BTreeMap<(i64, i64), i64> {
    let mut input_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for epoch in epochs {
        for &(a, b) in epoch {
            *input_acc.entry((a, b)).or_insert(0) += 1;
        }
    }
    // Apply filter + project
    let mut out = BTreeMap::new();
    for ((a, b), w) in &input_acc {
        if w > &0 && b * 2 > 10 {
            *out.entry((*a, b * 2)).or_insert(0i64) += w;
        }
    }
    out
}

#[tokio::test]
async fn lfs_pipeline_filter_project_writes_and_persists() {
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let sink = ViewSinkOp::new(db.clone(), OperatorId(42));
    let pipeline = make_pipeline();

    // Input epochs (a, b) with weight +1
    let input_epochs: Vec<Vec<(i64, i64)>> = vec![
        vec![(1, 6), (2, 3)], // (1,6): b*2=12>10 ✓; (2,3): b*2=6≤10 ✗
        vec![(3, 8), (4, 5)], // (3,8): b*2=16>10 ✓; (4,5): b*2=10=10 ✗ (not >)
        vec![(5, 7)],         // (5,7): b*2=14>10 ✓
    ];

    // Process each epoch through the pipeline and write to the sink.
    for (epoch_num, rows) in input_epochs.iter().enumerate() {
        let batch = ArrowZSet::from_ab_rows(rows, 1);
        let output = pipeline.process(batch).unwrap();
        if !output.is_empty() {
            sink.write_epoch(&output, epoch_num as u64).await.unwrap();
        }
    }

    // Flush to ensure durability.
    db.flush().await.unwrap();

    // Read back and verify.
    let stored = read_view_output(db.as_ref(), OperatorId(42), 2)
        .await
        .unwrap();
    // stored: Vec<(epoch, row_idx, [a, c], weight)>
    let stored_rows: BTreeMap<(i64, i64), i64> = stored
        .iter()
        .map(|(_, _, cols, w)| ((cols[0].as_i64().unwrap(), cols[1].as_i64().unwrap()), *w))
        .fold(BTreeMap::new(), |mut acc, (k, w)| {
            *acc.entry(k).or_insert(0) += w;
            acc
        });

    let expected = batch_expected(&input_epochs);
    assert_eq!(
        stored_rows, expected,
        "LFS pipeline output mismatch.\nstored: {:?}\nexpected: {:?}",
        stored_rows, expected
    );

    // ── Close and reopen: data must survive restart ─────────────────────

    // Drop the sink first (it holds an Arc clone), then close the db.
    drop(sink);
    Arc::try_unwrap(db)
        .ok()
        .expect("db has a single owner after dropping sink")
        .close()
        .await
        .unwrap();

    // Reopen
    let db2 = open_shard_db(&dir).await;
    let stored2 = read_view_output(db2.as_ref(), OperatorId(42), 2)
        .await
        .unwrap();
    let stored_rows2: BTreeMap<(i64, i64), i64> = stored2
        .iter()
        .map(|(_, _, cols, w)| ((cols[0].as_i64().unwrap(), cols[1].as_i64().unwrap()), *w))
        .fold(BTreeMap::new(), |mut acc, (k, w)| {
            *acc.entry(k).or_insert(0) += w;
            acc
        });

    assert_eq!(
        stored_rows2, expected,
        "LFS data did not survive close/reopen.\nstored after restart: {:?}\nexpected: {:?}",
        stored_rows2, expected
    );
}

/// Prove: no shuffle_inbox / shuffle_outbox keys written by the linear pipeline.
#[tokio::test]
async fn lfs_pipeline_no_shuffle_objects_written() {
    use rockstream_storage::ShardPrefix;
    let dir = TempDir::new().unwrap();
    let db = open_shard_db(&dir).await;
    let sink = ViewSinkOp::new(db.clone(), OperatorId(99));
    let pipeline = make_pipeline();

    let batch = ArrowZSet::from_ab_rows(&[(1, 6), (2, 7)], 1);
    let output = pipeline.process(batch).unwrap();
    sink.write_epoch(&output, 0).await.unwrap();
    db.flush().await.unwrap();

    // Check shuffle_inbox prefix is empty
    let (inbox, _) = db
        .scan_prefix_bounded(&[ShardPrefix::ShuffleInbox.as_byte()], 1_000)
        .await
        .unwrap();
    assert!(
        inbox.is_empty(),
        "No shuffle_inbox keys should be written by embedded pipeline"
    );

    // Check shuffle_outbox prefix is empty
    let (outbox, _) = db
        .scan_prefix_bounded(&[ShardPrefix::ShuffleOutbox.as_byte()], 1_000)
        .await
        .unwrap();
    assert!(
        outbox.is_empty(),
        "No shuffle_outbox keys should be written by embedded pipeline"
    );
}
