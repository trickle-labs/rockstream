use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use rockstream_connectors::{
    BackfillCursor, CoupledBatchDescriptor, CoupledTransactionBuilder, OffsetToken,
    SnapshotDeltaFence, SourceCheckpoint, SourceCheckpointStore,
};
use rockstream_storage::{
    keys::{ShardKeyEncoder, ShardPrefix},
    ShardDb, StorageError, WriteBatch,
};
use rockstream_types::ids::ConnectorId;
use tempfile::TempDir;

async fn open_mem_store(connector_id: ConnectorId) -> (Arc<ShardDb>, SourceCheckpointStore) {
    let object_store = Arc::new(InMemory::new());
    let db = Arc::new(
        ShardDb::builder("adapter-boundary-test", object_store)
            .build()
            .await
            .expect("open in-memory ShardDb"),
    );
    (db.clone(), SourceCheckpointStore::new(db, 0, connector_id))
}

async fn open_lfs_store(
    dir: &TempDir,
    connector_id: ConnectorId,
) -> (Arc<ShardDb>, SourceCheckpointStore) {
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("adapter-boundary-lfs", object_store)
            .build()
            .await
            .expect("open LFS ShardDb"),
    );
    (db.clone(), SourceCheckpointStore::new(db, 0, connector_id))
}

fn prepared_checkpoint(connector_id: ConnectorId, epoch: u64, token: &[u8]) -> SourceCheckpoint {
    SourceCheckpoint::prepared(connector_id, epoch, OffsetToken::new(token.to_vec()))
}

// ─── Mutation Test 1: State Mutation Missing ─────────────────────────────────

#[tokio::test]
async fn test_mutation_state_missing_refuses_durable_coupled_commit() {
    let connector_id = ConnectorId(9001);
    let (_db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 1, b"offset-state-missing");
    store.prepare(&checkpoint).await.unwrap();

    // 1. Raw WriteBatch: contains outputs, source marker, frontier, but NO state.
    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val");
    store.append_committed(&mut batch, &checkpoint).unwrap();
    batch.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());

    // Descriptor facts and validation
    let desc = CoupledBatchDescriptor::inspect(&batch);
    assert!(!desc.has_state);
    assert!(desc.has_outputs);
    assert!(desc.has_source_marker);
    assert!(desc.has_frontier);
    assert!(!desc.is_complete());

    let desc_err = desc.validate(false).unwrap_err();
    assert!(
        matches!(&desc_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing state mutation"),
        "unexpected error: {desc_err:?}"
    );

    // commit_m3 must fail closed
    let m3_err = store.commit_m3(batch).await.unwrap_err();
    assert!(
        matches!(&m3_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing state mutation"),
        "unexpected error: {m3_err:?}"
    );

    // 2. CoupledTransactionBuilder: missing state mutation
    let mut builder = CoupledTransactionBuilder::new();
    builder
        .add_view_output(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val")
        .add_source_marker(&store, &checkpoint)
        .unwrap()
        .add_frontier(1);

    let build_err = builder.clone().build(false).unwrap_err();
    assert!(
        matches!(&build_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing state mutation"),
        "unexpected error: {build_err:?}"
    );

    let tx_err = store
        .commit_coupled_transaction(builder, false)
        .await
        .unwrap_err();
    assert!(
        matches!(&tx_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing state mutation"),
        "unexpected error: {tx_err:?}"
    );

    // Refuses to claim durable coupled commit: highest_committed remains None
    assert_eq!(store.highest_committed().await.unwrap(), None);
}

// ─── Mutation Test 2: Output Mutation Missing ────────────────────────────────

#[tokio::test]
async fn test_mutation_output_missing_refuses_durable_coupled_commit() {
    let connector_id = ConnectorId(9002);
    let (_db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 2, b"offset-output-missing");
    store.prepare(&checkpoint).await.unwrap();

    // 1. Raw WriteBatch: contains state, source marker, frontier, but NO view output.
    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_val");
    store.append_committed(&mut batch, &checkpoint).unwrap();
    batch.put(&ShardKeyEncoder::frontier_key(), &2u64.to_be_bytes());

    // Descriptor facts and validation
    let desc = CoupledBatchDescriptor::inspect(&batch);
    assert!(desc.has_state);
    assert!(!desc.has_outputs);
    assert!(desc.has_source_marker);
    assert!(desc.has_frontier);
    assert!(!desc.is_complete());

    let desc_err = desc.validate(false).unwrap_err();
    assert!(
        matches!(&desc_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing view output mutation"),
        "unexpected error: {desc_err:?}"
    );

    // commit_m3 must fail closed
    let m3_err = store.commit_m3(batch).await.unwrap_err();
    assert!(
        matches!(&m3_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing view output mutation"),
        "unexpected error: {m3_err:?}"
    );

    // 2. CoupledTransactionBuilder: missing output mutation
    let mut builder = CoupledTransactionBuilder::new();
    builder
        .add_op_state(&[ShardPrefix::OpState.as_byte(), 1], b"state_val")
        .add_source_marker(&store, &checkpoint)
        .unwrap()
        .add_frontier(2);

    let build_err = builder.clone().build(false).unwrap_err();
    assert!(
        matches!(&build_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing view output mutation"),
        "unexpected error: {build_err:?}"
    );

    let tx_err = store
        .commit_coupled_transaction(builder, false)
        .await
        .unwrap_err();
    assert!(
        matches!(&tx_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing view output mutation"),
        "unexpected error: {tx_err:?}"
    );

    // Refuses to claim durable coupled commit
    assert_eq!(store.highest_committed().await.unwrap(), None);
}

// ─── Mutation Test 3: Source Marker Missing ──────────────────────────────────

#[tokio::test]
async fn test_mutation_source_marker_missing_refuses_durable_coupled_commit() {
    let connector_id = ConnectorId(9003);
    let (_db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 3, b"offset-marker-missing");
    store.prepare(&checkpoint).await.unwrap();

    // 1. Raw WriteBatch: contains state, output, frontier, but NO source checkpoint marker.
    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_val");
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val");
    batch.put(&ShardKeyEncoder::frontier_key(), &3u64.to_be_bytes());

    // Descriptor facts and validation
    let desc = CoupledBatchDescriptor::inspect(&batch);
    assert!(desc.has_state);
    assert!(desc.has_outputs);
    assert!(!desc.has_source_marker);
    assert!(desc.has_frontier);
    assert!(!desc.is_complete());

    let desc_err = desc.validate(false).unwrap_err();
    assert!(
        matches!(&desc_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing source marker mutation"),
        "unexpected error: {desc_err:?}"
    );

    // commit_m3 must fail closed
    let m3_err = store.commit_m3(batch).await.unwrap_err();
    assert!(
        matches!(&m3_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing source marker mutation"),
        "unexpected error: {m3_err:?}"
    );

    // 2. CoupledTransactionBuilder: missing source marker
    let mut builder = CoupledTransactionBuilder::new();
    builder
        .add_op_state(&[ShardPrefix::OpState.as_byte(), 1], b"state_val")
        .add_view_output(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val")
        .add_frontier(3);

    let build_err = builder.clone().build(false).unwrap_err();
    assert!(
        matches!(&build_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing source marker mutation"),
        "unexpected error: {build_err:?}"
    );

    let tx_err = store
        .commit_coupled_transaction(builder, false)
        .await
        .unwrap_err();
    assert!(
        matches!(&tx_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing source marker mutation"),
        "unexpected error: {tx_err:?}"
    );

    // Refuses to claim durable coupled commit
    assert_eq!(store.highest_committed().await.unwrap(), None);
}

// ─── Mutation Test 4: Frontier Mutation Missing ──────────────────────────────

#[tokio::test]
async fn test_mutation_frontier_missing_refuses_durable_coupled_commit() {
    let connector_id = ConnectorId(9004);
    let (_db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 4, b"offset-frontier-missing");
    store.prepare(&checkpoint).await.unwrap();

    // 1. Raw WriteBatch: contains state, output, source marker, but NO frontier update.
    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_val");
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val");
    store.append_committed(&mut batch, &checkpoint).unwrap();

    // Descriptor facts and validation
    let desc = CoupledBatchDescriptor::inspect(&batch);
    assert!(desc.has_state);
    assert!(desc.has_outputs);
    assert!(desc.has_source_marker);
    assert!(!desc.has_frontier);
    assert!(!desc.is_complete());

    let desc_err = desc.validate(false).unwrap_err();
    assert!(
        matches!(&desc_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing frontier mutation"),
        "unexpected error: {desc_err:?}"
    );

    // commit_m3 must fail closed
    let m3_err = store.commit_m3(batch).await.unwrap_err();
    assert!(
        matches!(&m3_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing frontier mutation"),
        "unexpected error: {m3_err:?}"
    );

    // 2. CoupledTransactionBuilder: missing frontier
    let mut builder = CoupledTransactionBuilder::new();
    builder
        .add_op_state(&[ShardPrefix::OpState.as_byte(), 1], b"state_val")
        .add_view_output(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val")
        .add_source_marker(&store, &checkpoint)
        .unwrap();

    let build_err = builder.clone().build(false).unwrap_err();
    assert!(
        matches!(&build_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing frontier mutation"),
        "unexpected error: {build_err:?}"
    );

    let tx_err = store
        .commit_coupled_transaction(builder, false)
        .await
        .unwrap_err();
    assert!(
        matches!(&tx_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing frontier mutation"),
        "unexpected error: {tx_err:?}"
    );

    // Refuses to claim durable coupled commit
    assert_eq!(store.highest_committed().await.unwrap(), None);
}

// ─── Mutation Test 5: Required Coupling Metadata Missing ─────────────────────

#[tokio::test]
async fn test_mutation_required_coupling_metadata_missing_refuses_durable_coupled_commit() {
    let connector_id = ConnectorId(9005);
    let (_db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 5, b"offset-meta-missing");
    store.prepare(&checkpoint).await.unwrap();

    // 1. Transaction builder with state, output, source marker, frontier, but NO coupling metadata.
    let mut builder = CoupledTransactionBuilder::new();
    builder
        .add_op_state(&[ShardPrefix::OpState.as_byte(), 1], b"state_val")
        .add_view_output(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val")
        .add_source_marker(&store, &checkpoint)
        .unwrap()
        .add_frontier(5);

    let desc = builder.descriptor();
    assert!(desc.is_complete());
    assert!(!desc.has_coupling_metadata);

    // When metadata is not required (require_metadata = false), validation passes
    assert!(desc.validate(false).is_ok());
    assert!(builder.clone().build(false).is_ok());

    // When metadata IS required (require_metadata = true), validation fails closed
    let desc_err = desc.validate(true).unwrap_err();
    assert!(
        matches!(&desc_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing coupling metadata mutation"),
        "unexpected error: {desc_err:?}"
    );

    let build_err = builder.clone().build(true).unwrap_err();
    assert!(
        matches!(&build_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing coupling metadata mutation"),
        "unexpected error: {build_err:?}"
    );

    let tx_err = store
        .commit_coupled_transaction(builder.clone(), true)
        .await
        .unwrap_err();
    assert!(
        matches!(&tx_err, StorageError::Unsupported(msg) if msg == "coupled batch validation failed: missing coupling metadata mutation"),
        "unexpected error: {tx_err:?}"
    );

    // Refuses to claim durable commit
    assert_eq!(store.highest_committed().await.unwrap(), None);

    // 2. Add coupling metadata: commit now succeeds with require_metadata = true
    let mut builder_with_meta = builder;
    let cursor = BackfillCursor::new(
        "customer_view",
        0,
        b"key-01".to_vec(),
        SnapshotDeltaFence::new(
            OffsetToken::new(b"snap-01".to_vec()),
            OffsetToken::new(b"live-01".to_vec()),
        ),
        5,
    );
    let mut cursor_batch = WriteBatch::new();
    store
        .append_backfill_cursor(&mut cursor_batch, &cursor)
        .unwrap();
    let (cursor_key, cursor_val) = match &cursor_batch.ops()[0] {
        rockstream_storage::BatchOp::Put { key, value } => (key.as_slice(), value.as_slice()),
        _ => unreachable!(),
    };
    builder_with_meta.add_coupling_metadata(cursor_key, cursor_val);

    let desc_with_meta = builder_with_meta.descriptor();
    assert!(desc_with_meta.is_complete());
    assert!(desc_with_meta.has_coupling_metadata);
    assert!(desc_with_meta.validate(true).is_ok());

    assert!(store
        .commit_coupled_transaction(builder_with_meta, true)
        .await
        .is_ok());

    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(checkpoint.committed())
    );
}

// ─── Positive Test: Complete Coupled Commit Succeeds ─────────────────────────

#[tokio::test]
async fn test_positive_complete_coupled_commit_succeeds() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(9006);
    let (db, store) = open_lfs_store(&dir, connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 10, b"offset-complete-pos");
    store.prepare(&checkpoint).await.unwrap();

    let mut builder = CoupledTransactionBuilder::new();
    let state_key = [ShardPrefix::OpState.as_byte(), 10, 20];
    let output_key = [ShardPrefix::ViewOutput.as_byte(), 30, 40];

    builder
        .add_op_state(&state_key, b"state_payload_10")
        .add_view_output(&output_key, b"output_payload_10")
        .add_source_marker(&store, &checkpoint)
        .unwrap()
        .add_frontier(10)
        .add_coupling_metadata(b"prefix/backfill_cursor/view_orders", b"cursor_payload_10");

    let desc = builder.descriptor();
    assert!(desc.has_state);
    assert!(desc.has_outputs);
    assert!(desc.has_source_marker);
    assert!(desc.has_frontier);
    assert!(desc.has_coupling_metadata);
    assert!(desc.is_complete());
    assert!(desc.validate(true).is_ok());

    // Commit coupled transaction succeeds
    store
        .commit_coupled_transaction(builder, true)
        .await
        .expect("commit_coupled_transaction must succeed");

    // Recoverable in active store
    let expected = checkpoint.committed();
    let recovered = store
        .highest_committed()
        .await
        .unwrap()
        .expect("committed checkpoint must be found");
    assert_eq!(recovered, expected);

    // Verify written values are readable in DB
    let read_state = store.db().get(&state_key).await.unwrap();
    assert_eq!(read_state.as_deref(), Some(b"state_payload_10".as_slice()));
    let read_output = store.db().get(&output_key).await.unwrap();
    assert_eq!(
        read_output.as_deref(),
        Some(b"output_payload_10".as_slice())
    );

    // Verify recovery across store restart / reopen from disk
    drop(store);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();

    let (_, recovered_store) = open_lfs_store(&dir, connector_id).await;
    let restart_recovered = recovered_store
        .highest_committed()
        .await
        .unwrap()
        .expect("checkpoint must survive restart");
    assert_eq!(restart_recovered, expected);
}

// ─── Negative Durability Tests: Flush and Write Failures ──────────────────────

#[tokio::test]
async fn test_storage_flush_failure_causes_commit_m3_to_fail_and_withhold_state() {
    let connector_id = ConnectorId(9007);
    let (db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 1, b"offset-flush-fail");
    store.prepare(&checkpoint).await.unwrap();

    // Construct a complete batch with all required components
    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_val");
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val");
    store.append_committed(&mut batch, &checkpoint).unwrap();
    batch.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());

    let desc = CoupledBatchDescriptor::inspect(&batch);
    assert!(desc.is_complete());

    // Inject storage flush failure
    db.set_fail_flushes(true);

    // commit_m3 must fail closed
    let err = store.commit_m3(batch).await.unwrap_err();
    assert!(
        matches!(&err, StorageError::Unsupported(msg) if msg.contains("injected storage flush failure")),
        "unexpected error: {err:?}"
    );

    // Durability condition was not satisfied, commit must not be reported as durable
    // Reset flush failure flag to allow clean reads
    db.set_fail_flushes(false);
}

#[tokio::test]
async fn test_storage_write_failure_causes_commit_m3_to_fail_and_withhold_state() {
    let connector_id = ConnectorId(9008);
    let (db, store) = open_mem_store(connector_id).await;
    let checkpoint = prepared_checkpoint(connector_id, 1, b"offset-write-fail");
    store.prepare(&checkpoint).await.unwrap();

    let mut batch = WriteBatch::new();
    batch.put(&[ShardPrefix::OpState.as_byte(), 1], b"state_val");
    batch.put(&[ShardPrefix::ViewOutput.as_byte(), 1], b"view_val");
    store.append_committed(&mut batch, &checkpoint).unwrap();
    batch.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());

    let desc = CoupledBatchDescriptor::inspect(&batch);
    assert!(desc.is_complete());

    // Inject storage write failure
    db.set_fail_writes(true);

    let err = store.commit_m3(batch).await.unwrap_err();
    assert!(
        matches!(&err, StorageError::Unsupported(msg) if msg.contains("injected storage write failure")),
        "unexpected error: {err:?}"
    );

    db.set_fail_writes(false);
    // Write failed, committed state is withheld: highest_committed remains None
    assert_eq!(store.highest_committed().await.unwrap(), None);
}
