use std::collections::BTreeMap;
use std::time::Duration;

use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_ops::sink::{read_view_output, ColumnValue};
use rockstream_plan::PlanNode;
use rockstream_runtime::client::start_worker_client_with_insecure_internal_dev_and_metadata;
use rockstream_types::config::JoinStrategy;
use rockstream_types::data_plane::{
    DeploymentColumn, DeploymentRequest, DeploymentSchema, OwnedEpochIntent, RuntimeRow,
    SourceDeltaRequest, DEPLOYMENT_DESCRIPTOR_VERSION,
};
use rockstream_types::ids::{OperatorId, WorkerId, WorkloadId};
use rockstream_types::topology::{
    ControlMessage, WorkerCapabilities, WorkerLocation, WorkerMessage,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::test]
async fn routed_shuffle_executes_on_worker_before_coordinator_completion() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let control_service = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_insecure_dev_mode();
    let control_handle = control_service.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();
    let location = WorkerLocation::new("test-host", "test-zone");
    let (worker1, task1) = start_worker_client_with_insecure_internal_dev_and_metadata(
        301,
        &control_addr,
        &dir.path().join("worker-301"),
        location.clone(),
        WorkerCapabilities::default(),
    )
    .await
    .unwrap();
    let (worker2, task2) = start_worker_client_with_insecure_internal_dev_and_metadata(
        302,
        &control_addr,
        &dir.path().join("worker-302"),
        location,
        WorkerCapabilities::default(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while worker1.worker_id() != Some(WorkerId(301))
            || worker2.worker_id() != Some(WorkerId(302))
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("workers did not register");

    let plan = PlanNode::ViewSink {
        view_name: "items_view".to_owned(),
        pk: vec![0],
        child: Box::new(PlanNode::Source {
            name: "items".to_owned(),
        }),
    };
    let workload_id = WorkloadId((1_u64 << 40) + 91);
    let request = DeploymentRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id,
        namespace: "public".to_owned(),
        workload_name: "test_view".to_owned(),
        plan_json: serde_json::to_string(&plan).unwrap(),
        join_strategy: JoinStrategy::Auto,
        schemas: vec![DeploymentSchema {
            relation: "items".to_owned(),
            columns: vec![DeploymentColumn {
                name: "id".to_owned(),
                data_type: "i64".to_owned(),
            }],
        }],
        frontier: 0,
        storage_root: dir.path().join("data").to_string_lossy().into_owned(),
        sink_operator_id: OperatorId(10),
        output_columns: vec!["id".to_owned()],
        primary_key: vec![0],
        merge_key_columns: vec![],
        routing_columns: BTreeMap::from([("items".to_owned(), 0)]),
    };

    let stream = TcpStream::connect(control_handle.addr).await.unwrap();
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    let message = WorkerMessage::DeployWorkload(request.clone());
    write
        .write_all((serde_json::to_string(&message).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::DeploymentReady {
            workload_id: actual,
            ..
        } => assert_eq!(actual, workload_id),
        other => panic!("unexpected deployment response: {other:?}"),
    }

    let expected: Vec<_> = (0_i64..64)
        .map(|id| (1_u64, vec![ColumnValue::Int64(id)], 1_i64))
        .collect();
    let rows = (0..64)
        .map(|id| RuntimeRow {
            values_tsv: id.to_string(),
            weight: 1,
        })
        .collect();
    let request_id = "owned-shuffle-e2e".to_owned();
    let message = WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: request_id.clone(),
        workload_id,
        epoch: 1,
        source: "items".to_owned(),
        rows,
    });
    let before = rockstream_types::metrics::read_shuffle_direct_bytes_total();
    write
        .write_all((serde_json::to_string(&message).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(15), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::SourceDeltaCommitted {
            request_id: actual,
            epoch,
        } => assert_eq!((actual, epoch), (request_id.clone(), 1)),
        other => panic!("unexpected source response: {other:?}"),
    }
    assert!(rockstream_types::metrics::read_shuffle_direct_bytes_total() > before);
    let mut actual = Vec::new();
    for worker in [&worker1, &worker2] {
        for lease in worker.leases() {
            let db = worker.get_shard_db(lease.shard_id).unwrap();
            actual.extend(
                read_view_output(&db, OperatorId(10), 1)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(epoch, _, row, weight)| (epoch, row, weight)),
            );
        }
    }
    actual.sort_by_key(|(_, row, _)| match &row[0] {
        ColumnValue::Int64(id) => *id,
        _ => panic!("unexpected output type"),
    });
    assert_eq!(actual, expected);

    write
        .write_all((serde_json::to_string(&message).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(15), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::SourceDeltaCommitted {
            request_id: actual,
            epoch,
        } => assert_eq!((actual, epoch), (request_id, 1)),
        other => panic!("unexpected committed replay response: {other:?}"),
    }
    let mut replayed = Vec::new();
    for worker in [&worker1, &worker2] {
        for lease in worker.leases() {
            let db = worker.get_shard_db(lease.shard_id).unwrap();
            replayed.extend(
                read_view_output(&db, OperatorId(10), 1)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(epoch, _, row, weight)| (epoch, row, weight)),
            );
        }
    }
    replayed.sort_by_key(|(_, row, _)| match &row[0] {
        ColumnValue::Int64(id) => *id,
        _ => panic!("unexpected output type"),
    });
    assert_eq!(replayed, expected);

    let target_shard = worker2.leases().into_iter().next().unwrap().shard_id;
    let target_db = worker2.get_shard_db(target_shard).unwrap();
    let before_rows = read_view_output(&target_db, OperatorId(10), 1)
        .await
        .unwrap();
    let id = match &before_rows
        .first()
        .expect("second shard must receive a row")
        .2[0]
    {
        ColumnValue::Int64(id) => *id,
        other => panic!("unexpected target row: {other:?}"),
    };
    target_db.set_fail_writes(true);
    let failure_request = WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: "owned-shuffle-write-failure".to_owned(),
        workload_id,
        epoch: 2,
        source: "items".to_owned(),
        rows: vec![RuntimeRow {
            values_tsv: id.to_string(),
            weight: -1,
        }],
    });
    write
        .write_all((serde_json::to_string(&failure_request).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(15), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    let detail = "[RS-0001] Internal error: unsupported operation: injected storage write failure; next_steps: report this issue";
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::OperationFailed {
            code,
            message,
            next_steps,
        } => {
            assert_eq!(code, "RS-0001");
            assert!(
                message == format!("worker execution failed: {detail}")
                    || message == format!("shuffle dispatch failed: {detail}"),
                "unexpected failure message: {message}"
            );
            assert_eq!(
                next_steps,
                "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying."
            );
        }
        other => panic!("unexpected failed source response: {other:?}"),
    }
    write
        .write_all(
            (serde_json::to_string(&WorkerMessage::ReadWorkload {
                version: rockstream_types::data_plane::WORKLOAD_SNAPSHOT_VERSION,
                workload_id,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::OperationFailed { code, message, next_steps } => assert_eq!(
            (code, message, next_steps),
            (
                "RS-0001".to_owned(),
                "workload requires recovery after failed execution".to_owned(),
                "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying.".to_owned(),
            )
        ),
        other => panic!("unexpected read response after failure: {other:?}"),
    }
    assert_eq!(
        rockstream_runtime::data_plane::DataPlaneClient::new(&control_addr).with_insecure_internal_dev()
            .read_workload(workload_id)
            .await
            .unwrap_err()
            .to_string(),
        "RS-0001: workload requires recovery after failed execution; Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying."
    );
    target_db.set_fail_writes(false);
    assert_eq!(
        read_view_output(&target_db, OperatorId(10), 1)
            .await
            .unwrap(),
        before_rows
    );
    let pending = target_db
        .get(&rockstream_storage::keys::ShardKeyEncoder::meta_key(
            b"owned-source-epoch",
        ))
        .await
        .unwrap()
        .unwrap();
    let pending: OwnedEpochIntent = serde_json::from_slice(&pending).unwrap();
    assert!(!pending.attempt_id.is_nil());
    assert_eq!(
        pending,
        OwnedEpochIntent {
            attempt_id: pending.attempt_id,
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "owned-shuffle-write-failure".to_owned(),
            workload_id,
            epoch: 2,
            source: "items".to_owned(),
            anchor_shard: target_shard,
            target_shards: vec![target_shard],
        }
    );
    let later = WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: "after-owned-failure".to_owned(),
        workload_id,
        epoch: 3,
        source: "items".to_owned(),
        rows: Vec::new(),
    });
    write
        .write_all((serde_json::to_string(&later).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::OperationFailed {
            code,
            message,
            next_steps,
        } => assert_eq!(
            (code, message, next_steps),
            (
                "RS-0001".to_owned(),
                "workload requires recovery after failed execution".to_owned(),
                "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying.".to_owned()
            )
        ),
        other => panic!("unexpected response after failure: {other:?}"),
    }
    let databases: Vec<_> = [&worker1, &worker2]
        .into_iter()
        .flat_map(|worker| {
            worker
                .leases()
                .into_iter()
                .filter_map(|lease| worker.get_shard_db(lease.shard_id))
        })
        .collect();
    task1.abort();
    task2.abort();
    let _ = task1.await;
    let _ = task2.await;
    for db in databases {
        db.close().await.unwrap();
    }
    drop((worker1, worker2, target_db, reader, write));
    control_handle.shutdown();

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let control_service = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_insecure_dev_mode();
    let control_handle = control_service.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();
    let location = WorkerLocation::new("replacement-host", "test-zone");
    let (worker1, task1) = start_worker_client_with_insecure_internal_dev_and_metadata(
        401,
        &control_addr,
        &dir.path().join("worker-401"),
        location.clone(),
        WorkerCapabilities::default(),
    )
    .await
    .unwrap();
    let (worker2, task2) = start_worker_client_with_insecure_internal_dev_and_metadata(
        402,
        &control_addr,
        &dir.path().join("worker-402"),
        location,
        WorkerCapabilities::default(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while worker1.worker_id() != Some(WorkerId(401))
            || worker2.worker_id() != Some(WorkerId(402))
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("workers did not reconnect");
    let stream = TcpStream::connect(control_handle.addr).await.unwrap();
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let message = WorkerMessage::DeployWorkload(request);
    write
        .write_all((serde_json::to_string(&message).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::OperationFailed {
            code,
            message,
            next_steps,
        } => assert_eq!(
            (code, message, next_steps),
            (
                "RS-0001".to_owned(),
                "workload requires recovery after failed execution".to_owned(),
                "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying.".to_owned()
            )
        ),
        other => panic!("unexpected deployment response after restart: {other:?}"),
    }
    assert_eq!(
        rockstream_runtime::data_plane::DataPlaneClient::new(&control_addr).with_insecure_internal_dev()
            .read_workload(workload_id)
            .await
            .unwrap_err()
            .to_string(),
        "RS-0001: workload requires recovery after failed execution; Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying."
    );
    let later = WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        request_id: "after-restart".to_owned(),
        workload_id,
        epoch: 3,
        source: "items".to_owned(),
        rows: Vec::new(),
    });
    write
        .write_all((serde_json::to_string(&later).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
    line.clear();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    match serde_json::from_str::<ControlMessage>(&line).unwrap() {
        ControlMessage::OperationFailed {
            code,
            message,
            next_steps,
        } => assert_eq!(
            (code, message, next_steps),
            (
                "RS-0001".to_owned(),
                "workload requires recovery after failed execution".to_owned(),
                "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying.".to_owned()
            )
        ),
        other => panic!("unexpected source response after restart: {other:?}"),
    }
    task1.abort();
    task2.abort();
    control_handle.shutdown();
}
