use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, S3Source, SnapshotDeltaFence,
    SourceCheckpoint, SourceCheckpointStore, SourceConnector,
};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;
use tempfile::TempDir;

const SNAPSHOT_ROWS: usize = 2_000_001;
const FILE_ROWS: usize = 10_000;

async fn store(dir: &TempDir) -> (Arc<ShardDb>, SourceCheckpointStore) {
    let db = Arc::new(
        ShardDb::builder(
            "backfill-resume-scale",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let checkpoints = SourceCheckpointStore::new(db.clone(), 0, ConnectorId(52_101));
    (db, checkpoints)
}

fn source(store: Arc<InMemory>, schema: Arc<Schema>) -> S3Source {
    S3Source::new(ConnectorId(52_101), schema).with_object_store(store, None)
}

async fn commit(
    checkpoints: &SourceCheckpointStore,
    epoch: u64,
    fence: &SnapshotDeltaFence,
    cursor: OffsetToken,
    phase: BackfillPhase,
    output: &[i64],
) {
    let checkpoint = SourceCheckpoint::prepared(ConnectorId(52_101), epoch, cursor.clone());
    checkpoints.prepare(&checkpoint).await.unwrap();
    let lifecycle = BackfillLifecycle::new(
        phase,
        BackfillCursor::new(
            "orders_mv",
            0,
            cursor.as_bytes().to_vec(),
            fence.clone(),
            epoch,
        ),
        0,
        SNAPSHOT_ROWS as u64,
        0,
        None,
    );
    let mut batch = WriteBatch::new();
    batch.put(
        format!("view_output/orders_mv/{epoch:08}").as_bytes(),
        &serde_json::to_vec(output).unwrap(),
    );
    checkpoints
        .append_committed(&mut batch, &checkpoint)
        .unwrap();
    checkpoints
        .append_backfill_lifecycle(&mut batch, &lifecycle)
        .unwrap();
    checkpoints.commit_m3(batch).await.unwrap();
}

#[tokio::test]
async fn multi_million_rows_resume_mid_snapshot_mid_interleave_mid_catch_up_exact_oracle() {
    let object_store = Arc::new(InMemory::new());
    for file in 0..SNAPSHOT_ROWS.div_ceil(FILE_ROWS) {
        let start = file * FILE_ROWS;
        let end = (start + FILE_ROWS).min(SNAPSHOT_ROWS);
        let mut body = String::new();
        for value in start..end {
            body.push_str(&format!("[{value}]\n"));
        }
        object_store
            .put(
                &Path::from(format!("snapshot/{file:04}.json")),
                body.into_bytes().into(),
            )
            .await
            .unwrap();
    }
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let dir = TempDir::new().unwrap();
    let (_db, checkpoints) = store(&dir).await;
    let mut source = source(object_store.clone(), schema.clone());
    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let mut after = None;
    let mut output = Vec::with_capacity(SNAPSHOT_ROWS + 3);
    let mut epoch = 0_u64;
    let mut restarted_mid_snapshot = false;

    loop {
        let mut stream = source
            .start_snapshot_bounded(&fence, after.clone(), None, FILE_ROWS)
            .await
            .unwrap();
        let Some(chunk) = stream.next() else { break };
        let (data, weights) = split_weight_column(&chunk.batch).unwrap();
        assert_eq!(weights, vec![1; data.num_rows()]);
        output.extend(
            data.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied(),
        );
        epoch += 1;
        commit(
            &checkpoints,
            epoch,
            &fence,
            chunk.resume_offset.clone(),
            BackfillPhase::Snapshotting,
            &output[output.len() - data.num_rows()..],
        )
        .await;
        after = Some(chunk.resume_offset);
        if !restarted_mid_snapshot && output.len() >= SNAPSHOT_ROWS / 2 {
            let recovered = checkpoints
                .backfill_lifecycle("orders_mv")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                recovered.cursor.last_key,
                after.as_ref().unwrap().as_bytes()
            );
            source = S3Source::new(ConnectorId(52_101), schema.clone())
                .with_object_store(object_store.clone(), None);
            after = Some(OffsetToken::new(recovered.cursor.last_key));
            restarted_mid_snapshot = true;
        }
    }
    assert_eq!(output, (0..SNAPSHOT_ROWS as i64).collect::<Vec<_>>());

    for live in 0..3_i64 {
        object_store
            .put(
                &Path::from(format!("live/{live:04}.json")),
                format!("[{}]\n", SNAPSHOT_ROWS as i64 + live)
                    .into_bytes()
                    .into(),
            )
            .await
            .unwrap();
    }
    let mut live_after = fence.live.clone();
    for live in 0..3 {
        let delta = source
            .poll_delta(live_after, usize::MAX, 1, None)
            .await
            .unwrap();
        let (data, weights) = split_weight_column(&delta.batches[0]).unwrap();
        assert_eq!(weights, vec![1]);
        let value = data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        output.push(value);
        epoch += 1;
        commit(
            &checkpoints,
            epoch,
            &fence,
            delta.new_offset.clone(),
            BackfillPhase::CatchingUp,
            &[value],
        )
        .await;
        live_after = delta.new_offset;
        if live == 0 {
            let recovered = checkpoints
                .backfill_lifecycle("orders_mv")
                .await
                .unwrap()
                .unwrap();
            source = S3Source::new(ConnectorId(52_101), schema.clone())
                .with_object_store(object_store.clone(), None);
            live_after = OffsetToken::new(recovered.cursor.last_key);
        }
    }
    let recovered = checkpoints
        .backfill_lifecycle("orders_mv")
        .await
        .unwrap()
        .unwrap();
    source =
        S3Source::new(ConnectorId(52_101), schema).with_object_store(object_store.clone(), None);
    assert_eq!(recovered.cursor.last_key, live_after.as_bytes());
    let drained = source
        .poll_delta(
            OffsetToken::new(recovered.cursor.last_key.clone()),
            usize::MAX,
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        (drained.batches.is_empty(), drained.new_offset),
        (true, live_after.clone()),
        "the restarted catch-up worker must not replay a committed live partition"
    );
    epoch += 1;
    commit(
        &checkpoints,
        epoch,
        &fence,
        live_after,
        BackfillPhase::Running,
        &[],
    )
    .await;

    assert_eq!(
        output,
        (0..SNAPSHOT_ROWS as i64 + 3).collect::<Vec<_>>(),
        "all fenced snapshot rows and post-fence deltas occur exactly once across all restarts"
    );
}
