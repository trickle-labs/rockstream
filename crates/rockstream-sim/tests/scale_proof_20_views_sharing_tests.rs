//! R1 structural proof for shared arrangements.
//!
//! Asserts:
//! 1. One consumer, 20 shared consumers, and 20 private arrangements each process 100K rows.
//! 2. Every consumer observes the complete output at the same frontier with zero source rescans.
//! 3. Shared state stays within 1.5x of one consumer and saves at least 80% versus private state.
//! 4. Logical and LFS bytes are measured after flush, and all arrangements are reclaimed.

use object_store::local::LocalFileSystem;
use rockstream_ops::view_attach::{AttachedView, AttachmentDeltaBuffer, ViewAttachmentMetrics};
use rockstream_sql::canonicalize::ExpressionNormalizer;
use rockstream_storage::arrangement_catalog::ArrangementCatalog;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_storage::ShardDb;
use rockstream_types::arrangement::{
    ArrangementSpec, CanonicalBinaryOp, CanonicalExpr, CanonicalType, CollationId,
    CollationVersion, NullSemantics, PartitioningSpec, SourceIdentity, TimeDomainSemantics,
};
use rockstream_types::batch::ZSetRow;
use rockstream_types::ids::{TenantId, ViewId};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const SOURCE_ROWS: usize = 100_000;
const FRONTIER: u64 = 100_000;

#[derive(Debug, PartialEq, Eq)]
struct FixtureStats {
    logical_bytes: u64,
    physical_bytes: u64,
    lfs_files: u64,
}

fn create_view_spec(view_index: usize) -> ArrangementSpec {
    // Variations in harmless syntax across 20 views:
    // 1. Column case & whitespace: "user_id" vs " USER_ID "
    let key_col = match view_index % 3 {
        0 => "user_id",
        1 => "USER_ID",
        _ => " user_id ",
    };

    // 2. Nested redundant casts vs single cast vs raw column (all normalize to same cast or col)
    let key_expr = if view_index.is_multiple_of(2) {
        CanonicalExpr::Cast {
            expr: Box::new(CanonicalExpr::Cast {
                expr: Box::new(CanonicalExpr::col(key_col)),
                target_type: CanonicalType::Int64,
            }),
            target_type: CanonicalType::Int64,
        }
    } else {
        CanonicalExpr::Cast {
            expr: Box::new(CanonicalExpr::col(key_col)),
            target_type: CanonicalType::Int64,
        }
    };

    // 3. Commutative predicate variations: x = 1 AND y = 2 vs y = 2 AND x = 1
    let pred = if view_index.is_multiple_of(2) {
        Some(CanonicalExpr::binary_op(
            CanonicalBinaryOp::And,
            CanonicalExpr::binary_op(
                CanonicalBinaryOp::Eq,
                CanonicalExpr::col("status"),
                CanonicalExpr::lit_int(1),
            ),
            CanonicalExpr::binary_op(
                CanonicalBinaryOp::Eq,
                CanonicalExpr::col("flag"),
                CanonicalExpr::lit_int(2),
            ),
        ))
    } else {
        Some(CanonicalExpr::binary_op(
            CanonicalBinaryOp::And,
            CanonicalExpr::binary_op(
                CanonicalBinaryOp::Eq,
                CanonicalExpr::col("flag"),
                CanonicalExpr::lit_int(2),
            ),
            CanonicalExpr::binary_op(
                CanonicalBinaryOp::Eq,
                CanonicalExpr::col("status"),
                CanonicalExpr::lit_int(1),
            ),
        ))
    };

    let source_name = if view_index.is_multiple_of(2) {
        "events"
    } else {
        "EVENTS"
    };
    let collation = if view_index.is_multiple_of(2) {
        "utf8_default"
    } else {
        "UTF8_DEFAULT"
    };

    let spec = ArrangementSpec {
        tenant_id: TenantId(1),
        security_policy_digest: [0u8; 32],
        source_identity: SourceIdentity::new(source_name),
        source_schema_generation: 1,
        key_expressions: vec![key_expr],
        key_types: vec![CanonicalType::Int64],
        value_projection: vec![CanonicalExpr::col("amount"), CanonicalExpr::col("user_id")],
        predicate: pred,
        null_semantics: NullSemantics::NullsFirst,
        decimal_scale: None,
        collation_identifier: CollationId(collation.to_string()),
        collation_version: CollationVersion(1),
        time_domain: TimeDomainSemantics::Utc,
        merge_law_id: MergeLawId(1),
        merge_law_version: MergeLawVersion(1),
        partitioning: PartitioningSpec::SingleShard(0),
    };

    ExpressionNormalizer::canonicalize_spec(&spec)
}

fn expected_state() -> BTreeMap<Vec<u8>, (Vec<u8>, i64)> {
    (0..SOURCE_ROWS)
        .map(|row| {
            (
                (row as i64).to_be_bytes().to_vec(),
                (((row as i64) * 3 + 7).to_be_bytes().to_vec(), 1),
            )
        })
        .collect()
}

fn lfs_usage(path: &Path) -> (u64, u64) {
    let mut pending = vec![path.to_path_buf()];
    let mut files = 0;
    let mut bytes = 0;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files += 1;
                bytes += metadata.len();
            }
        }
    }
    (files, bytes)
}

async fn run_fixture(consumers: usize, private_arrangements: bool) -> FixtureStats {
    let directory = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let db = ShardDb::builder("r1-shared-arrangement-proof", store)
        .build()
        .await
        .unwrap();
    let catalog = ArrangementCatalog::new();
    let expected = expected_state();
    let arrangement_count = if private_arrangements { consumers } else { 1 };
    let mut arrangement_ids = Vec::with_capacity(arrangement_count);
    let mut logical_bytes = 0;

    for arrangement_index in 0..arrangement_count {
        let view_ids = if private_arrangements {
            vec![ViewId((arrangement_index + 1) as u64)]
        } else {
            (1..=consumers).map(|id| ViewId(id as u64)).collect()
        };
        let mut spec = create_view_spec(arrangement_index + 1);
        if private_arrangements {
            spec.security_policy_digest[0] = (arrangement_index + 1) as u8;
        }

        let mut arrangement_id = None;
        for (consumer_index, view_id) in view_ids.iter().enumerate() {
            let (registered_id, is_new) = catalog.register_consumer(*view_id, spec.clone()).await;
            assert_eq!(is_new, consumer_index == 0);
            assert_eq!(arrangement_id.get_or_insert(registered_id), &registered_id);
        }
        let arrangement_id = arrangement_id.unwrap();
        arrangement_ids.push(arrangement_id);

        let mut trace = SharedArrangementTrace::new(spec);
        trace.commit_source_batch(0, FRONTIER, (0..SOURCE_ROWS).collect::<Vec<_>>(), |row| {
            ZSetRow::insert(
                (row as i64).to_be_bytes().to_vec(),
                ((row as i64) * 3 + 7).to_be_bytes().to_vec(),
            )
        });
        assert_eq!(trace.byte_size(), SOURCE_ROWS * 24);

        for view_id in &view_ids {
            let attached = AttachedView::attach(
                *view_id,
                &mut trace,
                FRONTIER,
                &mut AttachmentDeltaBuffer::new(0),
            )
            .unwrap();
            assert_eq!(attached.state, expected);
            assert_eq!(
                attached.metrics,
                ViewAttachmentMetrics {
                    source_scan_requests: 0,
                    pinned_frontier: FRONTIER,
                    snapshot_rows_loaded: SOURCE_ROWS,
                    buffered_delta_batches: 0,
                    buffered_delta_rows: 0,
                    live_attached_frontier: FRONTIER,
                }
            );
        }

        logical_bytes += trace.byte_size() as u64
            + (trace.consumer_frontiers.len()
                * (std::mem::size_of::<ViewId>() + std::mem::size_of::<u64>()))
                as u64;
        let trace_key = ShardKeyEncoder::encode(
            ShardPrefix::OpState,
            arrangement_index as u64,
            b"trace_data",
        );
        db.put(&trace_key, &serde_json::to_vec(&trace).unwrap())
            .await
            .unwrap();
        db.flush().await.unwrap();

        for (consumer_index, view_id) in view_ids.iter().enumerate() {
            trace.deregister_consumer(*view_id);
            assert_eq!(
                catalog
                    .deregister_consumer(*view_id, arrangement_id)
                    .await
                    .unwrap(),
                consumer_index + 1 == view_ids.len()
            );
        }
        assert!(trace.consumer_frontiers.is_empty());
        catalog
            .update_compaction_frontier(arrangement_id, FRONTIER)
            .await;
    }

    assert_eq!(
        catalog.physical_arrangements_count().await,
        arrangement_count
    );
    let mut reclaimed = catalog.reclaim_unreferenced_arrangements(FRONTIER).await;
    reclaimed.sort_unstable();
    arrangement_ids.sort_unstable();
    arrangement_ids.dedup();
    assert_eq!(reclaimed, arrangement_ids);
    assert_eq!(catalog.physical_arrangements_count().await, 0);

    db.close().await.unwrap();
    let (lfs_files, physical_bytes) = lfs_usage(directory.path());
    FixtureStats {
        logical_bytes,
        physical_bytes,
        lfs_files,
    }
}

#[tokio::test]
async fn test_scale_proof_20_views_sharing() {
    let single = run_fixture(1, false).await;
    let shared = run_fixture(20, false).await;
    let private = run_fixture(20, true).await;
    let trace_bytes = (SOURCE_ROWS * 24) as u64;

    assert_eq!(
        single.logical_bytes,
        trace_bytes + (std::mem::size_of::<ViewId>() + std::mem::size_of::<u64>()) as u64
    );
    assert_eq!(
        shared.logical_bytes,
        trace_bytes + (20 * (std::mem::size_of::<ViewId>() + std::mem::size_of::<u64>())) as u64
    );
    assert_eq!(private.logical_bytes, single.logical_bytes * 20);
    assert!(single.lfs_files > 0 && single.physical_bytes > 0);
    assert!(shared.lfs_files > 0 && shared.physical_bytes > 0);
    assert!(private.lfs_files > 0 && private.physical_bytes > 0);
    assert!(shared.logical_bytes * 2 <= single.logical_bytes * 3);
    assert!(shared.physical_bytes * 2 <= single.physical_bytes * 3);
    assert!(shared.logical_bytes * 5 <= private.logical_bytes);
    assert!(shared.physical_bytes * 5 <= private.physical_bytes);
}
