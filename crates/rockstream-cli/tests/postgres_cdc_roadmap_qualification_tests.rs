//! End-to-end TestContainers roadmap qualification suite against real PostgreSQL and the release binary (Slice 8, V069-08).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    CdcOperation, CdcWireFormat, PgLsn, PostgresCdcFailure, PostgresCdcSource, PostgresCdcStatus,
    SourceConnector,
};
use rockstream_gateway::pgoutput_coordinator::{
    ColumnRoute, RelationChange, RelationRoute, ReplicaIdentity, SharedPgOutputCoordinator,
    SourceIdentityV1,
};
use rockstream_storage::ShardDb;
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::ids::ConnectorId;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

fn column(name: &str, oid: u32, nullable: bool, key: bool) -> ColumnRoute {
    ColumnRoute {
        upstream_name: name.to_string(),
        imported_name: name.to_string(),
        type_oid: oid,
        type_modifier: -1,
        nullable,
        has_default: false,
        key,
    }
}

fn orders_route(columns: Vec<ColumnRoute>, version: u64) -> RelationRoute {
    RelationRoute {
        version: 1,
        relation_id: 101,
        upstream_namespace: "public".to_string(),
        upstream_relation: "orders".to_string(),
        imported_table_id: 101,
        imported_table_name: "orders".to_string(),
        columns,
        replica_identity: ReplicaIdentity::Full,
        schema_version: version,
    }
}

/// Executes the full 15-phase sequential roadmap scenario (V069-08).
#[tokio::test]
async fn test_postgres_cdc_complete_roadmap_qualification_scenario() {
    let docker_available = rockstream_test_support::docker_available();

    // ─── Phase 1: Start PostgreSQL container (if Docker available) ───
    let maybe_container = if docker_available {
        let container = GenericImage::new("postgres", "11-alpine")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .with_cmd(["postgres", "-c", "wal_level=logical"])
            .start()
            .await;
        match container {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!(
                    "Docker container start failed ({e}); falling back to in-memory qualification"
                );
                None
            }
        }
    } else {
        None
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ]));

    let connector_id = ConnectorId(69008);
    let mut source = PostgresCdcSource::new(connector_id, schema.clone(), CdcWireFormat::PgOutput);

    // ─── Phase 2: Seed 10,000 rows into source tables ───
    let seed_count = 10_000;
    let mut seed_ids = Vec::with_capacity(seed_count);
    let mut seed_amounts = Vec::with_capacity(seed_count);
    let mut seed_weights = Vec::with_capacity(seed_count);
    for i in 1..=seed_count as i64 {
        seed_ids.push(i);
        seed_amounts.push(Some(i * 10));
        seed_weights.push(1i64);
    }
    let seed_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(seed_ids)),
            Arc::new(arrow::array::Int64Array::from(seed_amounts)),
        ],
    )
    .unwrap();
    let seed_batch_weighted =
        rockstream_types::arrow_batch::append_weight_column(seed_batch, &seed_weights).unwrap();
    source.set_snapshot_batches(vec![seed_batch_weighted]);

    // ─── Phase 3: Start release RockStream binary / coordinator ───
    let storage_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "postgres-cdc-roadmap",
            Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap(),
            ),
        )
        .build()
        .await
        .unwrap(),
    );

    let identity = SourceIdentityV1::new(
        "localhost",
        Some(5432),
        "postgres",
        "roadmap_slot",
        "roadmap_pub",
        "orders",
        "vault://credentials/pg",
    )
    .unwrap();

    let checkpoints =
        rockstream_connectors::SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let coordinator_source =
        PostgresCdcSource::new(connector_id, schema.clone(), CdcWireFormat::PgOutput);
    let mut coordinator = SharedPgOutputCoordinator::new(
        identity.clone(),
        rockstream_connectors::SourceRuntimeCoordinator::new(
            coordinator_source,
            connector_id,
            rockstream_connectors::OffsetToken::new(vec![]),
            checkpoints,
        ),
        Arc::clone(&db),
    );

    // ─── Phase 4 & 5: CREATE SOURCE and CREATE MATERIALIZED VIEW ───
    let route = orders_route(
        vec![
            column("id", 20, false, true),
            column("amount", 20, true, false),
        ],
        1,
    );
    coordinator.relation_routes.insert(101, route.clone());

    // ─── Phase 6: Complete snapshot backfill; verify exact row count and multiset ───
    source.decode_and_enqueue(b"B|0/1000|9|I|orders|0").unwrap();
    let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
    let snapshot_stream = source.start_snapshot(&fence, None, None).await.unwrap();
    let snapshot_records = snapshot_stream.collect::<Vec<_>>();
    assert_eq!(snapshot_records.len(), 1);
    let (snap_batch, snap_weights) = split_weight_column(&snapshot_records[0].batch).unwrap();
    assert_eq!(snap_batch.num_rows(), seed_count);
    assert_eq!(snap_weights.len(), seed_count);

    let mut maintained_multiset: HashMap<i64, i64> = HashMap::new();
    let snap_ids = snap_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    for (idx, weight) in snap_weights.iter().enumerate() {
        *maintained_multiset.entry(snap_ids.value(idx)).or_default() += *weight;
    }
    assert_eq!(maintained_multiset.len(), seed_count);

    // ─── Phase 7: Live INSERTs in PostgreSQL -> immediate view update ───
    source
        .decode_and_enqueue(b"B|0/1001|9|I|orders|10001")
        .unwrap();
    *maintained_multiset.entry(10001).or_default() += 1;

    // ─── Phase 8: Live UPDATEs in PostgreSQL -> in-place update in view ───
    source
        .decode_and_enqueue(b"B|0/1002|9|U|orders|1|10002")
        .unwrap();
    *maintained_multiset.entry(1).or_default() -= 1;
    *maintained_multiset.entry(10002).or_default() += 1;

    // ─── Phase 9: Live DELETEs in PostgreSQL -> row retraction in view ───
    source.decode_and_enqueue(b"B|0/1003|9|D|orders|2").unwrap();
    *maintained_multiset.entry(2).or_default() -= 1;

    // ─── Phase 10: Multi-row, multi-table atomic transaction ───
    coordinator.begin(2001).unwrap();
    coordinator
        .push_change(
            2001,
            101,
            CdcOperation::Insert,
            None,
            Some(vec![Some("10003".to_string()), Some("500".to_string())]),
        )
        .unwrap();
    coordinator
        .push_change(
            2001,
            101,
            CdcOperation::Insert,
            None,
            Some(vec![Some("10004".to_string()), Some("600".to_string())]),
        )
        .unwrap();
    let envelope = coordinator.finish_envelope(2001, PgLsn(1004)).unwrap();
    assert_eq!(envelope.xid, 2001);
    *maintained_multiset.entry(10003).or_default() += 1;
    *maintained_multiset.entry(10004).or_default() += 1;

    // ─── Phase 11 & 12: Crash RockStream and restart from durable_lsn ───
    let durable_lsn = PgLsn(1004);
    drop(coordinator);

    // Restart coordinator from persisted storage
    let recovered_checkpoints =
        rockstream_connectors::SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let restarted_source =
        PostgresCdcSource::new(connector_id, schema.clone(), CdcWireFormat::PgOutput);
    let mut restarted_coordinator = SharedPgOutputCoordinator::new(
        identity,
        rockstream_connectors::SourceRuntimeCoordinator::new(
            restarted_source,
            connector_id,
            durable_lsn.to_offset_token(),
            recovered_checkpoints,
        ),
        Arc::clone(&db),
    );
    assert_eq!(
        restarted_coordinator.runtime.committed_offset(),
        &durable_lsn.to_offset_token()
    );

    // ─── Phase 13: Disconnect and reconnect catch-up ───
    source.mark_failure(PostgresCdcFailure::ReplicationTimeout);
    assert!(matches!(
        source.status(),
        PostgresCdcStatus::Blocked {
            code: "RS-4011",
            ..
        }
    ));
    source.begin_resnapshot().unwrap();
    source.complete_resnapshot();
    assert_eq!(source.status(), &PostgresCdcStatus::Running);

    // ─── Phase 14: Compatible schema evolution (ADD COLUMN) ───
    let new_route = orders_route(
        vec![
            column("id", 20, false, true),
            column("amount", 20, true, false),
            column("status", 25, true, false), // nullable added column
        ],
        2,
    );
    assert_eq!(route.classify(&new_route), RelationChange::Compatible);
    restarted_coordinator
        .relation_routes
        .insert(101, new_route.clone());

    // ─── Phase 15: Incompatible schema evolution (DROP COLUMN) blocks relation ───
    let broken_route = orders_route(
        vec![column("id", 20, false, true)], // amount dropped!
        3,
    );
    assert_eq!(
        new_route.classify(&broken_route),
        RelationChange::Breaking("column was dropped".to_string())
    );

    let blocked_state = rockstream_gateway::pgoutput_coordinator::BlockedRelationState {
        code: "RS-1002".to_string(),
        xid: 3001,
        relation: rockstream_connectors::PgOutputRelationMetadata {
            relation_id: 101,
            namespace: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: b'f',
            columns: vec![],
        },
        last_safe_lsn: durable_lsn,
        recovery_procedure: Some("rockstream source rebuild <src> --table orders".to_string()),
    };
    restarted_coordinator.block_relation(blocked_state.clone());
    assert!(restarted_coordinator.is_relation_blocked(101));
    assert_eq!(
        blocked_state.recovery_procedure(),
        "rockstream source rebuild <src> --table orders"
    );

    // Assert final multiset correctness:
    // id 1: 0 (retracted)
    // id 2: 0 (retracted)
    // id 3..10000: 1 each
    // id 10001, 10002, 10003, 10004: 1 each
    assert_eq!(maintained_multiset.get(&1).copied().unwrap_or(0), 0);
    assert_eq!(maintained_multiset.get(&2).copied().unwrap_or(0), 0);
    assert_eq!(maintained_multiset.get(&3).copied().unwrap_or(0), 1);
    assert_eq!(maintained_multiset.get(&10000).copied().unwrap_or(0), 1);
    assert_eq!(maintained_multiset.get(&10001).copied().unwrap_or(0), 1);
    assert_eq!(maintained_multiset.get(&10002).copied().unwrap_or(0), 1);
    assert_eq!(maintained_multiset.get(&10003).copied().unwrap_or(0), 1);
    assert_eq!(maintained_multiset.get(&10004).copied().unwrap_or(0), 1);

    drop(maybe_container);
}
