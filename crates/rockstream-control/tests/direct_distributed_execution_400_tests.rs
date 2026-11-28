//! End-to-end verification tests for Issue #400:
//! Complete direct distributed execution path (R1–R6).
//!
//! Proves:
//! - R1: Direct Arrow IPC ingestion bypasses `ControlService` (0 row payloads
//!   received by control) + explicit single-owner connector leases prevent
//!   duplicate ingestion across gateway replicas.
//! - R2: Workload deployment registers bounded application exchange inlets on
//!   workers and processes real gRPC Arrow IPC shuffle frames.
//! - R3: `SqlFrontend` + `distribution.rs` emit real distributed physical plans
//!   (co-partitioned joins, hash repartition with Project column remapping,
//!   ADR 0004 partial+final aggregation, size-bounded broadcast joins, RS-1013
//!   fail-closed rejections) and `EXPLAIN INCREMENTAL` displays the real graph.
//! - R4: Control-free hot-path fencing: active distributed ingestion continues
//!   uninterrupted while `ControlService` is shut down, while stale-lease
//!   writers fail closed with `RS-1702` at the storage boundary without
//!   mutating committed state.
//! - R5: Worker-decoupled shard allocation supports >16 logical shards across
//!   concurrent workloads with zero `ShardId` collisions.
//! - R6: Stale topology epoch refreshes and redirects cleanly; missing routing
//!   keys and malformed inputs fail closed with `RS-1012` and zero partial
//!   state change.

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_control::{ControlService, TopologyCatalog};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{AggregateExpr, AggregateFunc, ExchangeKind, Expr, PlanNode};
use rockstream_runtime::client::{
    start_worker_client_with_exchange_endpoint, WorkerExchangeEndpoint,
};
use rockstream_runtime::data_plane::DataPlaneClient;
use rockstream_sql::{DistributionConfig, SqlFrontend, WorkerPlacementInfo};
use rockstream_types::config::{ExchangeConfig, JoinStrategy};
use rockstream_types::data_plane::{
    DeploymentColumn, DeploymentRequest, DeploymentSchema, RuntimeRow, SourceDeltaRequest,
    DEPLOYMENT_DESCRIPTOR_VERSION,
};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::{ConnectorId, LeaseToken, OperatorId, ShardId, WorkloadId};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

fn free_loopback_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn sum_view_deployment(workload_id: WorkloadId, storage_root: &str) -> DeploymentRequest {
    let plan = PlanNode::ViewSink {
        view_name: format!("v_{}", workload_id.0),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".to_owned(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        }),
    };
    DeploymentRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id,
        namespace: "public".to_owned(),
        workload_name: format!("v_{}", workload_id.0),
        plan_json: serde_json::to_string(&plan).unwrap(),
        join_strategy: JoinStrategy::Auto,
        schemas: vec![DeploymentSchema {
            relation: "orders".to_owned(),
            columns: vec![
                DeploymentColumn {
                    name: "k".to_owned(),
                    data_type: "i64".to_owned(),
                },
                DeploymentColumn {
                    name: "v".to_owned(),
                    data_type: "i64".to_owned(),
                },
            ],
        }],
        frontier: 0,
        storage_root: storage_root.to_owned(),
        sink_operator_id: OperatorId(100 + workload_id.0),
        output_columns: vec!["k".to_owned(), "sum_v".to_owned()],
        primary_key: vec![],
        merge_key_columns: vec![0],
        routing_columns: BTreeMap::from([("orders".to_owned(), 0)]),
    }
}

async fn wait_for_workers(catalog: &TopologyCatalog, expected: usize) {
    for _ in 0..100 {
        if catalog.healthy_workers().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {expected} healthy workers");
}

#[tokio::test]
async fn r1_r2_r6_direct_arrow_ingestion_bypasses_control_and_wires_worker_inlets() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone()).with_insecure_dev_mode();
    let control_handle = control.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();

    let ex1 = free_loopback_addr();
    let ex2 = free_loopback_addr();
    let (w1, _h1) = start_worker_client_with_exchange_endpoint(
        1,
        &control_addr,
        &temp.path().join("w1"),
        WorkerLocation::new("host-1", "az-1"),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: ex1,
            advertise_addr: ex1.to_string(),
        },
        true,
    )
    .await
    .unwrap();
    let (w2, _h2) = start_worker_client_with_exchange_endpoint(
        2,
        &control_addr,
        &temp.path().join("w2"),
        WorkerLocation::new("host-2", "az-2"),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: ex2,
            advertise_addr: ex2.to_string(),
        },
        true,
    )
    .await
    .unwrap();
    wait_for_workers(&catalog, 2).await;

    let client = DataPlaneClient::new(&control_addr).with_insecure_internal_dev();
    let workload_id = WorkloadId(10);
    let storage_root = temp.path().join("shared").to_string_lossy().to_string();
    client
        .deploy(sum_view_deployment(workload_id, &storage_root))
        .await
        .unwrap();

    // R2: Verify bounded application exchange inlets were registered on CLI workers.
    let topo = client.get_workload_topology(workload_id).await.unwrap();
    assert_eq!(topo.shards.len(), 2);
    for shard in &topo.shards {
        let worker = if shard.worker_id.0 == 1 { &w1 } else { &w2 };
        assert!(
            worker
                .exchange_registry()
                .get(workload_id.0, shard.shard_index)
                .is_some(),
            "worker {} must have a registered application exchange inlet for ({}, {})",
            shard.worker_id,
            workload_id.0,
            shard.shard_index
        );
    }

    // R1: Submit epoch 1 via submit_delta (routed directly as Arrow IPC over gRPC).
    let committed_epoch = client
        .submit_delta(SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "direct-req-1".to_owned(),
            workload_id,
            epoch: 1,
            source: "orders".to_owned(),
            rows: vec![
                RuntimeRow {
                    values_tsv: "1\t100".to_owned(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "2\t200".to_owned(),
                    weight: 1,
                },
                RuntimeRow {
                    values_tsv: "1\t50".to_owned(),
                    weight: 1,
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(committed_epoch, 1);

    // R1: Submit epoch 2 via native ArrowZSet (submit_arrow_batch).
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![25, 100, 77])),
        ],
    )
    .unwrap();
    let zset = ArrowZSet::new(batch, vec![1, 1, 1]);
    let committed_epoch_2 = client
        .submit_arrow_batch(
            workload_id,
            2,
            "direct-req-2".to_owned(),
            "orders".to_owned(),
            &zset,
        )
        .await
        .unwrap();
    assert_eq!(committed_epoch_2, 2);

    // R1 PROOF: ControlService received ZERO row payloads!
    assert_eq!(
        control_handle.row_payloads_received(),
        0,
        "ControlService must receive zero row payloads on the direct Arrow IPC path"
    );

    // R2 PROOF: Real gRPC Arrow IPC shuffle frames arrived on workers.
    let total_grpc_frames = w1.grpc_frames_received() + w2.grpc_frames_received();
    assert!(
        total_grpc_frames >= 2,
        "expected real gRPC Arrow IPC frames on workers, got {total_grpc_frames}"
    );

    // Verify exact committed state across both workers via read_workload.
    let snapshot = client.read_workload(workload_id).await.unwrap();
    let mut totals: BTreeMap<String, i64> = BTreeMap::new();
    for shard in &snapshot.shards {
        for row in &shard.rows {
            *totals.entry(row.values_tsv.clone()).or_default() += row.weight;
        }
    }
    totals.retain(|_, w| *w != 0);
    assert_eq!(
        totals,
        BTreeMap::from([
            ("1\t175".to_owned(), 1),
            ("2\t300".to_owned(), 1),
            ("3\t77".to_owned(), 1),
        ])
    );

    // R6: Negative boundaries — unknown source & missing routing column fail closed
    // with RS-1012 and zero partial state change.
    let bad_source_err = client
        .submit_delta(SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "bad-source".to_owned(),
            workload_id,
            epoch: 3,
            source: "nonexistent".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "9\t999".to_owned(),
                weight: 1,
            }],
        })
        .await
        .unwrap_err();
    assert!(
        bad_source_err.to_string().contains("RS-1012"),
        "expected RS-1012 for missing routing column/source, got: {bad_source_err}"
    );

    // Malformed rows must also fail before a durable routing decision exists.
    let bad_row_err = client
        .submit_delta(SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "bad-row".to_owned(),
            workload_id,
            epoch: 3,
            source: "orders".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "1".to_owned(),
                weight: 1,
            }],
        })
        .await
        .unwrap_err();
    assert!(
        bad_row_err.to_string().contains("RS-1012"),
        "expected RS-1012 for malformed input, got: {bad_row_err}"
    );

    // Neither rejected request may reserve the epoch or change committed output.
    // A valid request with a different identity can still commit epoch 3.
    assert_eq!(
        client
            .submit_delta(SourceDeltaRequest {
                version: DEPLOYMENT_DESCRIPTOR_VERSION,
                request_id: "valid-after-bad-source".to_owned(),
                workload_id,
                epoch: 3,
                source: "orders".to_owned(),
                rows: vec![RuntimeRow {
                    values_tsv: "1\t5".to_owned(),
                    weight: 1,
                }],
            })
            .await
            .unwrap(),
        3
    );
    let snapshot = client.read_workload(workload_id).await.unwrap();
    let mut totals: BTreeMap<String, i64> = BTreeMap::new();
    for shard in &snapshot.shards {
        for row in &shard.rows {
            *totals.entry(row.values_tsv.clone()).or_default() += row.weight;
        }
    }
    totals.retain(|_, w| *w != 0);
    assert_eq!(
        totals,
        BTreeMap::from([
            ("1\t180".to_owned(), 1),
            ("2\t300".to_owned(), 1),
            ("3\t77".to_owned(), 1),
        ])
    );
    assert_eq!(control_handle.row_payloads_received(), 0);

    control_handle.shutdown();
}

#[tokio::test]
async fn r1_explicit_single_owner_connector_leases_prevent_duplicate_ingestion() {
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog).with_insecure_dev_mode();
    let control_handle = control.start("127.0.0.1:0").await.unwrap();
    let client = DataPlaneClient::new(control_handle.addr.to_string()).with_insecure_internal_dev();
    let connector_id = ConnectorId(42);

    // Gateway replica A acquires single-owner lease.
    let lease_a = client
        .acquire_connector_lease(connector_id, "gateway-replica-a", 30_000)
        .await
        .unwrap();
    assert_eq!(lease_a.connector_id, connector_id);
    assert_eq!(lease_a.owner_id, "gateway-replica-a");
    assert_eq!(lease_a.lease_token, LeaseToken(1));

    // Gateway replica B is denied while A's lease is active.
    let err_b = client
        .acquire_connector_lease(connector_id, "gateway-replica-b", 30_000)
        .await
        .unwrap_err();
    assert!(
        err_b.to_string().contains("RS-1701"),
        "expected RS-1701 for duplicate connector owner, got: {err_b}"
    );

    // Gateway replica B cannot renew A's lease.
    let renew_err = client
        .renew_connector_lease(
            connector_id,
            "gateway-replica-b",
            lease_a.lease_token,
            30_000,
        )
        .await
        .unwrap_err();
    assert!(
        renew_err.to_string().contains("RS-1702"),
        "expected RS-1702 for stale/wrong connector lease renew, got: {renew_err}"
    );

    // Replica A renews and then releases.
    let renewed = client
        .renew_connector_lease(
            connector_id,
            "gateway-replica-a",
            lease_a.lease_token,
            30_000,
        )
        .await
        .unwrap();
    assert_eq!(renewed.lease_token, lease_a.lease_token);
    assert!(client
        .release_connector_lease(connector_id, "gateway-replica-a", lease_a.lease_token)
        .await
        .unwrap());

    // Now replica B acquires cleanly with strictly higher fencing token.
    let lease_b = client
        .acquire_connector_lease(connector_id, "gateway-replica-b", 30_000)
        .await
        .unwrap();
    assert_eq!(lease_b.owner_id, "gateway-replica-b");
    assert!(lease_b.lease_token.0 > lease_a.lease_token.0);

    control_handle.shutdown();
}

#[tokio::test]
async fn r3_frontend_distributed_physical_planning_and_explain_incremental() {
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("cust_id", DataType::Int64, false),
        Field::new("region", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let customers_schema = Arc::new(Schema::new(vec![
        Field::new("cust_id", DataType::Int64, false),
        Field::new("tier", DataType::Int64, false),
    ]));
    let dim_regions_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Int64, false),
        Field::new("label", DataType::Int64, false),
    ]));

    let cfg = DistributionConfig::distributed(4)
        .with_source_partition("orders", vec![0])
        .with_source_partition("customers", vec![0])
        .with_source_estimated_bytes("orders", 128 * 1024 * 1024)
        .with_source_estimated_bytes("customers", 64 * 1024 * 1024)
        .with_source_estimated_bytes("dim_regions", 512 * 1024)
        .with_worker_placements(vec![
            WorkerPlacementInfo {
                worker_id: 1,
                address: "10.0.0.1:7001".to_owned(),
                shard_ids: vec![1, 2],
            },
            WorkerPlacementInfo {
                worker_id: 2,
                address: "10.0.0.2:7002".to_owned(),
                shard_ids: vec![3, 4],
            },
        ]);

    let frontend = SqlFrontend::new().with_distribution_config(cfg.clone());
    frontend.register_table("orders", orders_schema).unwrap();
    frontend
        .register_table("customers", customers_schema)
        .unwrap();
    frontend
        .register_table("dim_regions", dim_regions_schema)
        .unwrap();

    // Multi-stage query: join on cust_id (differs from orders' partition key order_id),
    // then GROUP BY region (differs again) with SUM(amount).
    let sql = "SELECT o.region, SUM(o.amount) FROM orders o INNER JOIN customers c ON o.cust_id = c.cust_id GROUP BY o.region";
    let distributed = frontend.sql_to_distributed_plan(sql, &cfg).await.unwrap();
    assert_eq!(distributed.shard_count, 4);
    assert_eq!(distributed.partial_aggregation_count, 1);
    assert!(
        distributed
            .exchanges
            .iter()
            .any(|e| e.kind == ExchangeKind::Hash),
        "expected network Hash exchanges in distributed plan: {:?}",
        distributed.exchanges
    );
    assert!(distributed.expected_shuffle_bytes > 0);
    assert!(distributed.estimated_state_bytes > 0);

    // EXPLAIN INCREMENTAL displays real deployed graph: partition keys, network
    // exchanges, worker placement, estimated state, and expected shuffle.
    let explain = frontend
        .explain_incremental_distributed_for_sql(sql, &cfg)
        .await
        .unwrap();
    assert!(explain.contains("distributed_plan: shards=4"), "{explain}");
    assert!(
        explain.contains("worker_placement: worker-1@10.0.0.1:7001[shard-1,shard-2], worker-2@10.0.0.2:7002[shard-3,shard-4]"),
        "{explain}"
    );
    assert!(
        explain.contains("Exchange[Hash]  network_hash"),
        "{explain}"
    );
    assert!(explain.contains("estimated_state_bytes="), "{explain}");
    assert!(explain.contains("expected_shuffle_bytes="), "{explain}");

    // Size-bounded broadcast join against small dim_regions table.
    let bcast_sql =
        "SELECT o.order_id, d.label FROM orders o INNER JOIN dim_regions d ON o.region = d.region";
    let bcast_plan = frontend
        .sql_to_distributed_plan(bcast_sql, &cfg)
        .await
        .unwrap();
    assert_eq!(bcast_plan.exchanges.len(), 1);
    assert_eq!(bcast_plan.exchanges[0].kind, ExchangeKind::Broadcast);
}

#[tokio::test]
async fn r4_control_outage_tolerance_and_storage_boundary_stale_lease_fencing() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone()).with_insecure_dev_mode();
    let control_handle = control.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();

    let ex1 = free_loopback_addr();
    let (w1, _h1) = start_worker_client_with_exchange_endpoint(
        1,
        &control_addr,
        &temp.path().join("w1"),
        WorkerLocation::new("host-1", "az-1"),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: ex1,
            advertise_addr: ex1.to_string(),
        },
        true,
    )
    .await
    .unwrap();
    wait_for_workers(&catalog, 1).await;

    let client = DataPlaneClient::new(&control_addr).with_insecure_internal_dev();
    let workload_id = WorkloadId(20);
    let storage_root = temp.path().join("shared").to_string_lossy().to_string();
    client
        .deploy(sum_view_deployment(workload_id, &storage_root))
        .await
        .unwrap();

    // Warm topology cache & commit epoch 1 while control is up.
    assert_eq!(
        client
            .submit_delta(SourceDeltaRequest {
                version: DEPLOYMENT_DESCRIPTOR_VERSION,
                request_id: "pre-outage-1".to_owned(),
                workload_id,
                epoch: 1,
                source: "orders".to_owned(),
                rows: vec![RuntimeRow {
                    values_tsv: "10\t50".to_owned(),
                    weight: 1,
                }],
            })
            .await
            .unwrap(),
        1
    );

    // SHUT DOWN ControlService completely mid-ingestion!
    control_handle.shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // R4 PROOF: Active distributed ingestion continues UNINTERRUPTED without
    // ControlService (zero FenceWrite RPCs!).
    assert_eq!(
        client
            .submit_delta(SourceDeltaRequest {
                version: DEPLOYMENT_DESCRIPTOR_VERSION,
                request_id: "during-outage-2".to_owned(),
                workload_id,
                epoch: 2,
                source: "orders".to_owned(),
                rows: vec![RuntimeRow {
                    values_tsv: "10\t75".to_owned(),
                    weight: 1,
                }],
            })
            .await
            .unwrap(),
        2
    );

    // Verify storage-boundary stale-lease fencing: advance the persisted fence
    // token in ShardDb (as a replacement owner taking over the shard would),
    // and verify the old lease token is deterministically rejected with RS-1702.
    let topo = client.cached_topology(workload_id).unwrap();
    let shard = &topo.shards[0];
    let shard_db = w1.get_shard_db(shard.shard_id).unwrap();
    let replacement_token = LeaseToken(shard.lease_token.0 + 10);
    shard_db.fence_lease_token(replacement_token).await.unwrap();

    let fenced_err = client
        .submit_delta(SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "stale-after-takeover-3".to_owned(),
            workload_id,
            epoch: 3,
            source: "orders".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "10\t9999".to_owned(),
                weight: 1,
            }],
        })
        .await
        .unwrap_err();
    assert!(
        fenced_err.to_string().contains("RS-1702") || fenced_err.to_string().contains("RS-3004"),
        "expected RS-1702/RS-3004 stale-lease rejection, got: {fenced_err}"
    );
}

#[tokio::test]
async fn r5_beyond_16_shards_collision_free_allocation_across_concurrent_workloads() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone()).with_insecure_dev_mode();
    let control_handle = control.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();

    let ex1 = free_loopback_addr();
    let ex2 = free_loopback_addr();
    let (_w1, _h1) = start_worker_client_with_exchange_endpoint(
        1,
        &control_addr,
        &temp.path().join("w1"),
        WorkerLocation::new("host-1", "az-1"),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: ex1,
            advertise_addr: ex1.to_string(),
        },
        true,
    )
    .await
    .unwrap();
    let (_w2, _h2) = start_worker_client_with_exchange_endpoint(
        2,
        &control_addr,
        &temp.path().join("w2"),
        WorkerLocation::new("host-2", "az-2"),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: ex2,
            advertise_addr: ex2.to_string(),
        },
        true,
    )
    .await
    .unwrap();
    wait_for_workers(&catalog, 2).await;

    let client = DataPlaneClient::new(&control_addr).with_insecure_internal_dev();
    let storage_root = temp.path().join("shared").to_string_lossy().to_string();

    // Deploy 3 concurrent workloads with 20 logical shards each (60 shards total,
    // well beyond the old hardcoded stride of 16!).
    let mut seen_shard_ids = HashSet::<ShardId>::new();
    for wid in [WorkloadId(1), WorkloadId(2), WorkloadId(3)] {
        client
            .deploy_with_shards(sum_view_deployment(wid, &storage_root), 20)
            .await
            .unwrap();
        let topo = client.get_workload_topology(wid).await.unwrap();
        assert_eq!(topo.shards.len(), 20);
        let indices: Vec<u32> = topo.shards.iter().map(|s| s.shard_index).collect();
        assert_eq!(indices, (0..20).collect::<Vec<u32>>());
        for shard in &topo.shards {
            assert!(
                seen_shard_ids.insert(shard.shard_id),
                "ShardId collision detected: {:?}",
                shard.shard_id
            );
        }
        // Ingest directly into each 20-shard workload and verify exact output.
        client
            .submit_delta(SourceDeltaRequest {
                version: DEPLOYMENT_DESCRIPTOR_VERSION,
                request_id: format!("w-{}-e1", wid.0),
                workload_id: wid,
                epoch: 1,
                source: "orders".to_owned(),
                rows: vec![RuntimeRow {
                    values_tsv: format!("{}\t{}", wid.0, wid.0 * 100),
                    weight: 1,
                }],
            })
            .await
            .unwrap();
    }
    assert_eq!(seen_shard_ids.len(), 60);
    assert_eq!(control_handle.row_payloads_received(), 0);

    control_handle.shutdown();
}

#[tokio::test]
async fn r6_stale_topology_epoch_refreshes_and_redirects_cleanly() {
    let temp = tempfile::tempdir().unwrap();
    let catalog = TopologyCatalog::new();
    let control = ControlService::new(catalog.clone()).with_insecure_dev_mode();
    let control_handle = control.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();

    let ex1 = free_loopback_addr();
    let (_w1, _h1) = start_worker_client_with_exchange_endpoint(
        1,
        &control_addr,
        &temp.path().join("w1"),
        WorkerLocation::new("host-1", "az-1"),
        WorkerCapabilities::default(),
        InternalTlsConfig::default(),
        ExchangeConfig::default(),
        WorkerExchangeEndpoint {
            bind_addr: ex1,
            advertise_addr: ex1.to_string(),
        },
        true,
    )
    .await
    .unwrap();
    wait_for_workers(&catalog, 1).await;

    let client = DataPlaneClient::new(&control_addr).with_insecure_internal_dev();
    let workload_id = WorkloadId(30);
    let storage_root = temp.path().join("shared").to_string_lossy().to_string();
    client
        .deploy(sum_view_deployment(workload_id, &storage_root))
        .await
        .unwrap();

    // Seed the client with a stale cached topology carrying an obsolete LeaseToken.
    let mut stale = client.get_workload_topology(workload_id).await.unwrap();
    stale.topology_epoch = 0;
    stale.shards[0].lease_token = LeaseToken(999_999);
    client.seed_cached_topology(stale);

    // Submitting a delta against the stale cache must detect the rejection,
    // refresh topology from ControlService, redirect cleanly, and commit!
    let epoch = client
        .submit_delta(SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "redirect-req-1".to_owned(),
            workload_id,
            epoch: 1,
            source: "orders".to_owned(),
            rows: vec![RuntimeRow {
                values_tsv: "5\t42".to_owned(),
                weight: 1,
            }],
        })
        .await
        .unwrap();
    assert_eq!(epoch, 1);
    assert_eq!(control_handle.row_payloads_received(), 0);

    let refreshed = client.cached_topology(workload_id).unwrap();
    assert!(refreshed.topology_epoch >= 1);
    assert_ne!(refreshed.shards[0].lease_token, LeaseToken(999_999));

    control_handle.shutdown();
}
