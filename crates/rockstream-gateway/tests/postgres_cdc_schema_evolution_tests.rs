use std::sync::Arc;

use arrow::datatypes::Schema;
use object_store::memory::InMemory;
use rockstream_connectors::{
    CdcWireFormat, OffsetToken, PostgresCdcSource, SourceCheckpointStore, SourceRuntimeCoordinator,
};
use rockstream_gateway::pgoutput_coordinator::{
    ColumnRoute, RelationChange, RelationRoute, ReplicaIdentity, SharedPgOutputCoordinator,
    SourceIdentityV1,
};
use rockstream_storage::{ShardDb, WriteBatch};

fn column(
    name: &str,
    oid: u32,
    modifier: i32,
    nullable: bool,
    has_default: bool,
    key: bool,
) -> ColumnRoute {
    ColumnRoute {
        upstream_name: name.to_string(),
        imported_name: name.to_string(),
        type_oid: oid,
        type_modifier: modifier,
        nullable,
        has_default,
        key,
    }
}

fn route(columns: Vec<ColumnRoute>) -> RelationRoute {
    RelationRoute {
        version: 1,
        relation_id: 52,
        upstream_namespace: "public".to_string(),
        upstream_relation: "orders".to_string(),
        imported_table_id: 52,
        imported_table_name: "orders".to_string(),
        columns,
        replica_identity: ReplicaIdentity::Full,
        schema_version: 1,
    }
}

fn base_i32() -> RelationRoute {
    route(vec![column("id", 23, -1, false, false, true)])
}

async fn assert_compatible_history(old: RelationRoute, mut next: RelationRoute) {
    assert_eq!(old.classify(&next), RelationChange::Compatible);
    next.schema_version = 2;
    let identity = SourceIdentityV1::new(
        "db.example",
        None,
        "postgres",
        "orders_slot",
        "orders_pub",
        "postgres",
        "none://trusted",
    )
    .unwrap();
    let connector_id = identity.connector_id();
    let db = Arc::new(
        ShardDb::builder("pgoutput-schema-history", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let source = PostgresCdcSource::new(
        connector_id,
        Arc::new(Schema::empty()),
        CdcWireFormat::PgOutput,
    );
    let checkpoints =
        SourceCheckpointStore::new(Arc::clone(&db), connector_id.0 as u128, connector_id);
    let coordinator = SharedPgOutputCoordinator::new(
        identity,
        SourceRuntimeCoordinator::new(source, connector_id, OffsetToken::new(vec![]), checkpoints),
        Arc::clone(&db),
    );
    let mut batch = WriteBatch::new();
    coordinator
        .append_route_updates(&mut batch, &[next.clone()])
        .unwrap();
    db.write_batch(batch).await.unwrap();
    assert_eq!(
        db.scan_prefix(format!("connector/{}/pgoutput/schema_history/", connector_id.0).as_bytes())
            .await
            .unwrap()
            .into_iter()
            .map(|(_, value)| serde_json::from_slice::<RelationRoute>(&value).unwrap())
            .collect::<Vec<_>>(),
        vec![next]
    );
}

#[tokio::test]
async fn pgoutput_schema_add_nullable_i32_records_history_and_streams_exact() {
    let old = base_i32();
    let new = route(vec![
        column("id", 23, -1, false, false, true),
        column("note", 25, -1, true, false, false),
    ]);
    assert_compatible_history(old, new).await;
}

#[tokio::test]
async fn pgoutput_schema_add_default_text_records_history_and_streams_exact() {
    let old = route(vec![column("id", 20, -1, false, false, true)]);
    let new = route(vec![
        column("id", 20, -1, false, false, true),
        column("state", 25, -1, false, true, false),
    ]);
    assert_compatible_history(old, new).await;
}

#[tokio::test]
async fn pgoutput_schema_widen_i32_i64_records_history_and_streams_exact() {
    let old = base_i32();
    let new = route(vec![column("id", 20, -1, false, false, true)]);
    assert_compatible_history(old, new).await;
}

#[tokio::test]
async fn pgoutput_schema_widen_text_records_history_and_streams_exact() {
    let old = route(vec![column("value", 1043, 8, false, false, false)]);
    let new = route(vec![column("value", 1043, 32, false, false, false)]);
    assert_compatible_history(old, new).await;
}

#[test]
fn pgoutput_schema_drop_i64_blocks_rs1002_without_rows() {
    let old = route(vec![
        column("id", 20, -1, false, false, true),
        column("value", 20, -1, false, false, false),
    ]);
    assert_eq!(
        old.classify(&route(vec![column("id", 20, -1, false, false, true)])),
        RelationChange::Breaking("column was dropped".to_string())
    );
}

#[test]
fn pgoutput_schema_narrow_i64_i32_blocks_rs1002_without_rows() {
    let old = route(vec![column("id", 20, -1, false, false, true)]);
    assert_eq!(
        old.classify(&base_i32()),
        RelationChange::Breaking("column type narrowed or changed".to_string())
    );
}

#[test]
fn pgoutput_schema_key_type_change_blocks_rs1002_without_rows() {
    let old = route(vec![column("id", 20, -1, false, false, true)]);
    assert_eq!(
        old.classify(&route(vec![column("id", 25, -1, false, false, true)])),
        RelationChange::Breaking("column type narrowed or changed".to_string())
    );
}

#[test]
fn pgoutput_schema_reorder_or_rename_blocks_rs1002_without_rows() {
    let old = route(vec![
        column("id", 20, -1, false, false, true),
        column("value", 25, -1, false, false, false),
    ]);
    assert_eq!(
        old.classify(&route(vec![
            column("value", 25, -1, false, false, false),
            column("id", 20, -1, false, false, true),
        ])),
        RelationChange::Breaking(
            "column was renamed, reordered, or changed key identity".to_string()
        )
    );
}

#[tokio::test]
async fn test_schema_evolution_add_nullable_column_compatible() {
    let old = base_i32();
    let new = route(vec![
        column("id", 23, -1, false, false, true),
        column("c2", 25, -1, true, false, false),
    ]);
    assert_eq!(old.classify(&new), RelationChange::Compatible);
    assert_compatible_history(old, new).await;
}

#[test]
fn test_schema_evolution_rename_column_requires_rebuild() {
    let old = route(vec![
        column("c1", 20, -1, false, false, true),
        column("value", 25, -1, false, false, false),
    ]);
    let new = route(vec![
        column("c1_new", 20, -1, false, false, true),
        column("value", 25, -1, false, false, false),
    ]);
    assert_eq!(
        old.classify(&new),
        RelationChange::Breaking(
            "column was renamed, reordered, or changed key identity".to_string()
        )
    );
}

#[tokio::test]
async fn test_schema_evolution_type_widening_compatible() {
    let old = route(vec![column("c1", 23, -1, false, false, true)]);
    let new = route(vec![column("c1", 20, -1, false, false, true)]);
    assert_eq!(old.classify(&new), RelationChange::Compatible);
    assert_compatible_history(old, new).await;
}

#[test]
fn test_schema_evolution_drop_referenced_col_requires_rebuild() {
    let old = route(vec![
        column("id", 20, -1, false, false, true),
        column("c1", 25, -1, false, false, false),
    ]);
    let new = route(vec![column("id", 20, -1, false, false, true)]);
    assert_eq!(
        old.classify(&new),
        RelationChange::Breaking("column was dropped".to_string())
    );
}

#[test]
fn test_schema_evolution_primary_key_change_blocks() {
    let old = route(vec![
        column("c1", 20, -1, false, false, true),
        column("c2", 20, -1, false, false, false),
    ]);
    let new = route(vec![
        column("c1", 20, -1, false, false, false),
        column("c2", 20, -1, false, false, true),
    ]);
    assert_eq!(
        old.classify(&new),
        RelationChange::Breaking(
            "column was renamed, reordered, or changed key identity".to_string()
        )
    );
}

#[tokio::test]
async fn test_schema_evolution_decision_table_and_relation_isolation() {
    use rockstream_connectors::{CdcOperation, PgLsn, PgOutputRelationMetadata};
    use rockstream_gateway::pgoutput_coordinator::BlockedRelationState;

    let identity = SourceIdentityV1::new(
        "db.example",
        None,
        "postgres",
        "isolated_slot",
        "isolated_pub",
        "postgres",
        "none://trusted",
    )
    .unwrap();
    let connector_id = identity.connector_id();
    let db = Arc::new(
        ShardDb::builder("isolation-test", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let source = PostgresCdcSource::new(
        connector_id,
        Arc::new(Schema::empty()),
        CdcWireFormat::PgOutput,
    );
    let checkpoints =
        SourceCheckpointStore::new(Arc::clone(&db), connector_id.0 as u128, connector_id);
    let mut coordinator = SharedPgOutputCoordinator::new(
        identity,
        SourceRuntimeCoordinator::new(source, connector_id, OffsetToken::new(vec![]), checkpoints),
        Arc::clone(&db),
    );

    let route_orders = RelationRoute {
        version: 1,
        relation_id: 101,
        upstream_namespace: "public".to_string(),
        upstream_relation: "orders".to_string(),
        imported_table_id: 101,
        imported_table_name: "orders".to_string(),
        columns: vec![column("id", 23, -1, false, false, true)],
        replica_identity: ReplicaIdentity::Full,
        schema_version: 1,
    };
    let route_payments = RelationRoute {
        version: 1,
        relation_id: 102,
        upstream_namespace: "public".to_string(),
        upstream_relation: "payments".to_string(),
        imported_table_id: 102,
        imported_table_name: "payments".to_string(),
        columns: vec![column("id", 23, -1, false, false, true)],
        replica_identity: ReplicaIdentity::Full,
        schema_version: 1,
    };

    coordinator
        .relation_routes
        .insert(101, route_orders.clone());
    coordinator
        .relation_routes
        .insert(102, route_payments.clone());

    let blocked_orders = BlockedRelationState {
        code: "RS-1002".to_string(),
        xid: 500,
        relation: PgOutputRelationMetadata {
            relation_id: 101,
            namespace: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: b'f',
            columns: vec![],
        },
        last_safe_lsn: PgLsn(100),
        recovery_procedure: Some("rockstream source rebuild <src> --table orders".to_string()),
    };
    coordinator.block_relation(blocked_orders.clone());

    assert!(coordinator.is_relation_blocked(101));
    assert!(!coordinator.is_relation_blocked(102));
    assert_eq!(
        blocked_orders.recovery_procedure(),
        "rockstream source rebuild <src> --table orders"
    );

    coordinator.begin(501).unwrap();
    coordinator
        .push_change(
            501,
            101,
            CdcOperation::Insert,
            None,
            Some(vec![Some("1".to_string())]),
        )
        .unwrap();
    coordinator
        .push_change(
            501,
            102,
            CdcOperation::Insert,
            None,
            Some(vec![Some("10".to_string())]),
        )
        .unwrap();

    let envelope = coordinator.finish_envelope(501, PgLsn(200)).unwrap();
    assert_eq!(envelope.xid, 501);
}
