//! Control Plane Boundary Decoupling Tests (v0.67 Slice 1 / Phase 3a).
//!
//! Asserts that the control plane never buffers or stores row payloads or query
//! output history; it forwards routed frames without retaining their contents.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use rockstream_control::service::ControlService;
use rockstream_control::shard::ShardManager;
use rockstream_control::topology::TopologyCatalog;
use rockstream_types::config::JoinStrategy;
use rockstream_types::data_plane::{
    DeploymentRequest, RuntimeExchangeMessage, RuntimeOutputDelta, RuntimeRow, SourceDeltaRequest,
};
use rockstream_types::explain::{IdentifiedOperatorStats, OperatorStats};
use rockstream_types::ids::{OperatorId, WorkerId, WorkloadId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};

#[tokio::test]
async fn source_delta_waits_for_each_routed_shard() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let catalog = TopologyCatalog::new();
        let registrations: Vec<_> = [1, 2]
            .into_iter()
            .map(|id| {
                WorkerRegistration::new(
                    WorkerId(id),
                    NodeRole::Worker,
                    format!("127.0.0.1:{}", 9000 + id),
                    CapacityHeadroom::FULL,
                )
            })
            .collect();
        for registration in &registrations {
            catalog.register(registration);
        }
        let shard_manager = ShardManager::new();
        let service =
            ControlService::new(catalog.clone()).with_shard_manager(shard_manager.clone());

        let handle = service
            .with_insecure_dev_mode()
            .start("127.0.0.1:0")
            .await
            .unwrap();

        let mut workers = Vec::new();
        for registration in registrations {
            let stream = TcpStream::connect(handle.addr).await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            write_worker_message(&mut write, &WorkerMessage::Register(registration.clone())).await;
            loop {
                if let ControlMessage::Registered { worker_id } =
                    read_control_message(&mut reader).await
                {
                    assert_eq!(worker_id, registration.worker_id);
                    break;
                }
            }
            workers.push((reader, write, registration.worker_id));
        }
        let stream = TcpStream::connect(handle.addr).await.unwrap();
        let (read, mut client_write) = stream.into_split();
        let mut client_reader = BufReader::new(read);
        let workload_id = WorkloadId(42);
        let request = DeploymentRequest {
            version: 1,
            workload_id,
            plan_json: "{}".to_owned(),
            join_strategy: JoinStrategy::Auto,
            schemas: vec![],
            frontier: 0,
            storage_root: "/tmp/storage".to_owned(),
            sink_operator_id: OperatorId(10),
            output_columns: vec!["val".to_owned()],
            primary_key: vec![0],
            merge_key_columns: vec![0],
            routing_columns: BTreeMap::from([("src".to_owned(), 0)]),
        };
        write_worker_message(
            &mut client_write,
            &WorkerMessage::DeployWorkload(request.clone()),
        )
        .await;
        for (reader, write, worker_id) in &mut workers {
            let descriptor = loop {
                if let ControlMessage::Deploy { descriptor } = read_control_message(reader).await {
                    break descriptor;
                }
            };
            write_worker_message(
                write,
                &WorkerMessage::DeploymentReady {
                    version: 1,
                    workload_id,
                    shard_id: descriptor.shard.shard_id,
                    worker_id: *worker_id,
                    process_id: 9999,
                    operator_ids: vec![OperatorId(10)],
                    frontier: 1,
                },
            )
            .await;
        }
        assert!(matches!(
            read_control_message(&mut client_reader).await,
            ControlMessage::DeploymentReady { workload_id: id, .. } if id == workload_id
        ));

        let rows: Vec<_> = (0..64)
            .map(|index| RuntimeRow {
                values_tsv: format!("{index}\tvalue-{index}"),
                weight: 1,
            })
            .collect();
        let request_id = "duplicate-progress".to_owned();
        write_worker_message(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: request_id.clone(),
                workload_id,
                epoch: 2,
                source: "src".to_owned(),
                rows: rows.clone(),
            }),
        )
        .await;
        let mut frames = Vec::new();
        for (reader, _, _) in &mut workers {
            let frame = loop {
                if let ControlMessage::Execute { frame } = read_control_message(reader).await {
                    break frame;
                }
            };
            assert!(!frame.rows.is_empty());
            frames.push(frame);
        }
        assert_ne!(frames[0].shard_id, frames[1].shard_id);
        let mut actual_rows: Vec<_> = frames.iter().flat_map(|frame| frame.rows.clone()).collect();
        let mut expected_rows = rows;
        actual_rows.sort_by(|a, b| a.values_tsv.cmp(&b.values_tsv));
        expected_rows.sort_by(|a, b| a.values_tsv.cmp(&b.values_tsv));
        assert_eq!(actual_rows, expected_rows);

        for rows in [frames[0].rows.clone(), Vec::new()] {
            write_worker_message(
                &mut client_write,
                &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                    version: 1,
                    request_id: "concurrent".to_owned(),
                    workload_id,
                    epoch: 3,
                    source: "src".to_owned(),
                    rows,
                }),
            )
            .await;
            match read_control_message(&mut client_reader).await {
                ControlMessage::OperationFailed {
                    code,
                    message,
                    next_steps,
                } => assert_eq!(
                    (code, message, next_steps),
                    (
                        "RS-0001".to_owned(),
                        "another source delta is already in flight for this workload".to_owned(),
                        "Retry after the workload and workers are ready.".to_owned(),
                    )
                ),
                other => panic!("unexpected concurrent response: {other:?}"),
            }
        }
        write_worker_message(
            &mut client_write,
            &WorkerMessage::DeployWorkload(request.clone()),
        )
        .await;
        match read_control_message(&mut client_reader).await {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => assert_eq!(
                (code, message, next_steps),
                (
                    "RS-0001".to_owned(),
                    "another source delta is already in flight for this workload".to_owned(),
                    "Retry after the workload and workers are ready.".to_owned(),
                )
            ),
            other => panic!("unexpected concurrent redeployment response: {other:?}"),
        }

        let progress = |frame: &RuntimeExchangeMessage| WorkerMessage::ExecutionProgress {
            output: RuntimeOutputDelta {
                attempt_id: None,
                version: frame.version,
                request_id: frame.request_id.clone(),
                workload_id: frame.workload_id,
                shard_id: frame.shard_id,
                epoch: frame.epoch,
                operator_id: frame.operator_id,
                lease_token: frame.lease_token,
                source: frame.source.clone(),
                rows: frame.rows.clone(),
            },
            input_rows: frame.rows.len() as u64,
            output_rows: frame.rows.len() as u64,
        };
        write_worker_message(&mut workers[0].1, &progress(&frames[0])).await;
        write_worker_message(&mut workers[0].1, &progress(&frames[0])).await;
        let early = tokio::time::timeout(
            Duration::from_millis(250),
            read_control_message(&mut client_reader),
        )
        .await;
        assert!(
            early.is_err(),
            "duplicate progress completed the request: {early:?}"
        );

        write_worker_message(&mut workers[1].1, &progress(&frames[1])).await;
        let completion = tokio::time::timeout(
            Duration::from_secs(5),
            read_control_message(&mut client_reader),
        )
        .await
        .unwrap();
        match completion {
            ControlMessage::SourceDeltaCommitted {
                request_id: actual,
                epoch,
            } => {
                assert_eq!((actual, epoch), (request_id, 2));
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let failed_request_id = "failed-progress".to_owned();
        write_worker_message(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: failed_request_id.clone(),
                workload_id,
                epoch: 3,
                source: "src".to_owned(),
                rows: expected_rows,
            }),
        )
        .await;
        let mut failed_frames = Vec::new();
        for (reader, _, _) in &mut workers {
            let frame = loop {
                if let ControlMessage::Execute { frame } = read_control_message(reader).await {
                    break frame;
                }
            };
            failed_frames.push(frame);
        }
        let failed_frame = &failed_frames[0];
        let failure = serde_json::json!({
            "type": "execution_failed",
            "output": {
                "version": failed_frame.version,
                "request_id": failed_request_id,
                "workload_id": failed_frame.workload_id,
                "shard_id": failed_frame.shard_id,
                "epoch": failed_frame.epoch,
                "operator_id": failed_frame.operator_id,
                "lease_token": failed_frame.lease_token,
                "source": failed_frame.source,
                "rows": []
            },
            "message": "test delivery rejected"
        });
        let mut stale_failure = failure.clone();
        stale_failure["output"]["epoch"] = serde_json::json!(failed_frame.epoch - 1);
        workers[0]
            .1
            .write_all(format!("{stale_failure}\n").as_bytes())
            .await
            .unwrap();
        assert!(tokio::time::timeout(
            Duration::from_millis(250),
            read_control_message(&mut client_reader),
        )
        .await
        .is_err());
        workers[0]
            .1
            .write_all(format!("{failure}\n").as_bytes())
            .await
            .unwrap();
        let failure_reply = tokio::time::timeout(
            Duration::from_secs(2),
            read_control_message(&mut client_reader),
        )
        .await
        .expect("worker failure did not reach the request");
        match failure_reply {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => assert_eq!(
                (code, message, next_steps),
                (
                    "RS-0001".to_owned(),
                    "worker execution failed: test delivery rejected".to_owned(),
                    "Reconcile the affected workload before retrying.".to_owned(),
                )
            ),
            other => panic!("unexpected failure response: {other:?}"),
        }
        write_worker_message(&mut workers[1].1, &progress(&failed_frames[1])).await;
        assert!(tokio::time::timeout(
            Duration::from_millis(250),
            read_control_message(&mut client_reader),
        )
        .await
        .is_err());
        write_worker_message(
            &mut client_write,
            &WorkerMessage::ReadWorkload { workload_id },
        )
        .await;
        match read_control_message(&mut client_reader).await {
            ControlMessage::WorkloadSnapshot { snapshot } => {
                let mut frontiers: Vec<_> = snapshot
                    .workers
                    .into_iter()
                    .map(|worker| (worker.worker_id, worker.frontier))
                    .collect();
                frontiers.sort_by_key(|(worker_id, _)| *worker_id);
                let mut expected = vec![(workers[0].2, 2), (workers[1].2, 2)];
                expected.sort_by_key(|(worker_id, _)| *worker_id);
                assert_eq!(frontiers, expected);
            }
            other => panic!("unexpected workload response: {other:?}"),
        }
        for rows in [failed_frame.rows.clone(), Vec::new()] {
            write_worker_message(
                &mut client_write,
                &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                    version: 1,
                    request_id: "after-failure".to_owned(),
                    workload_id,
                    epoch: 4,
                    source: "src".to_owned(),
                    rows,
                }),
            )
            .await;
            let response = tokio::time::timeout(
                Duration::from_secs(2),
                read_control_message(&mut client_reader),
            )
            .await
            .expect("later request did not receive a failure");
            match response {
                ControlMessage::OperationFailed {
                    code,
                    message,
                    next_steps,
                } => assert_eq!(
                    (code, message, next_steps),
                    (
                        "RS-0001".to_owned(),
                        "workload requires recovery after failed execution".to_owned(),
                        "Reconcile the affected workload before retrying.".to_owned(),
                    )
                ),
                other => panic!("unexpected later response: {other:?}"),
            }
        }

        write_worker_message(&mut client_write, &WorkerMessage::DeployWorkload(request)).await;
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            read_control_message(&mut client_reader),
        )
        .await
        .expect("redeployment did not receive a failure");
        match response {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => assert_eq!(
                (code, message, next_steps),
                (
                    "RS-0001".to_owned(),
                    "workload requires recovery after failed execution".to_owned(),
                    "Reconcile the affected workload before retrying.".to_owned(),
                )
            ),
            other => panic!("unexpected redeployment response: {other:?}"),
        }

        let workload_id = WorkloadId(43);
        let request = DeploymentRequest {
            version: 1,
            workload_id,
            plan_json: "{}".to_owned(),
            join_strategy: JoinStrategy::Auto,
            schemas: vec![],
            frontier: 0,
            storage_root: "/tmp/storage".to_owned(),
            sink_operator_id: OperatorId(10),
            output_columns: vec!["val".to_owned()],
            primary_key: vec![0],
            merge_key_columns: vec![0],
            routing_columns: BTreeMap::from([("src".to_owned(), 0)]),
        };
        write_worker_message(&mut client_write, &WorkerMessage::DeployWorkload(request)).await;
        for (reader, write, worker_id) in &mut workers {
            let descriptor = loop {
                if let ControlMessage::Deploy { descriptor } = read_control_message(reader).await {
                    break descriptor;
                }
            };
            write_worker_message(
                write,
                &WorkerMessage::DeploymentReady {
                    version: 1,
                    workload_id,
                    shard_id: descriptor.shard.shard_id,
                    worker_id: *worker_id,
                    process_id: 9999,
                    operator_ids: vec![OperatorId(10)],
                    frontier: 1,
                },
            )
            .await;
        }
        assert!(matches!(
            read_control_message(&mut client_reader).await,
            ControlMessage::DeploymentReady { workload_id: id, .. } if id == workload_id
        ));
        let rows = (0..64)
            .map(|index| RuntimeRow {
                values_tsv: format!("{index}\tvalue-{index}"),
                weight: 1,
            })
            .collect();
        write_worker_message(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: "disconnected".to_owned(),
                workload_id,
                epoch: 2,
                source: "src".to_owned(),
                rows,
            }),
        )
        .await;
        let mut disconnected_frames = Vec::new();
        for (reader, _, _) in &mut workers {
            loop {
                if let ControlMessage::Execute { frame } = read_control_message(reader).await {
                    disconnected_frames.push(frame);
                    break;
                }
            }
        }
        write_worker_message(&mut workers[1].1, &progress(&disconnected_frames[1])).await;
        workers[0].1.shutdown().await.unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            read_control_message(&mut client_reader),
        )
        .await
        .expect("disconnected worker did not fail the request");
        match response {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => assert_eq!(
                (code, message, next_steps),
                (
                    "RS-0001".to_owned(),
                    "worker disconnected before source delta completed".to_owned(),
                    "Reconcile the affected workload before retrying.".to_owned()
                )
            ),
            other => panic!("unexpected disconnect response: {other:?}"),
        }
        write_worker_message(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: "after-disconnect".to_owned(),
                workload_id,
                epoch: 3,
                source: "src".to_owned(),
                rows: Vec::new(),
            }),
        )
        .await;
        match read_control_message(&mut client_reader).await {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => assert_eq!(
                (code, message, next_steps),
                (
                    "RS-0001".to_owned(),
                    "workload requires recovery after failed execution".to_owned(),
                    "Reconcile the affected workload before retrying.".to_owned()
                )
            ),
            other => panic!("unexpected post-disconnect response: {other:?}"),
        }
        handle.shutdown();
    })
    .await
    .expect("source delta fixture did not finish");
}

async fn read_control_message<R: AsyncBufRead + Unpin>(reader: &mut R) -> ControlMessage {
    let mut line = String::new();
    assert!(reader.read_line(&mut line).await.unwrap() > 0);
    serde_json::from_str(&line).unwrap()
}

async fn write_worker_message<W: AsyncWrite + Unpin>(writer: &mut W, message: &WorkerMessage) {
    let mut line = serde_json::to_string(message).unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
}

#[tokio::test]
async fn test_control_plane_retains_zero_row_payloads_or_output_history() {
    let catalog = TopologyCatalog::new();
    let reg = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9001".to_string(),
        CapacityHeadroom::FULL,
    );
    catalog.register(&reg);

    let shard_manager = ShardManager::new();
    let service = ControlService::new(catalog.clone()).with_shard_manager(shard_manager.clone());

    let handle = service
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let bound_addr = handle.addr;

    // 1. Worker connects and registers
    let worker_stream = TcpStream::connect(bound_addr).await.unwrap();
    let (worker_read, mut worker_write) = worker_stream.into_split();
    let mut worker_reader = BufReader::new(worker_read);

    let reg_msg = WorkerMessage::Register(reg);
    let wire_reg = serde_json::to_string(&reg_msg).unwrap() + "\n";
    worker_write.write_all(wire_reg.as_bytes()).await.unwrap();
    let mut line = String::new();
    worker_reader.read_line(&mut line).await.unwrap();
    let reg_resp: ControlMessage = serde_json::from_str(&line).unwrap();
    assert!(
        matches!(reg_resp, ControlMessage::Registered { worker_id } if worker_id == WorkerId(1)),
        "Expected Registered, got {reg_resp:?}"
    );

    // Consume TopologyChanged notification on worker connection
    line.clear();
    worker_reader.read_line(&mut line).await.unwrap();
    let top_resp: ControlMessage = serde_json::from_str(&line).unwrap();
    assert!(
        matches!(top_resp, ControlMessage::TopologyChanged { .. }),
        "Expected TopologyChanged, got {top_resp:?}"
    );

    // 2. Client connects and deploys a workload
    let client_stream = TcpStream::connect(bound_addr).await.unwrap();
    let (client_read, mut client_write) = client_stream.into_split();
    let mut client_reader = BufReader::new(client_read);

    let workload_id = WorkloadId(42);
    let deploy_req = DeploymentRequest {
        version: 1,
        workload_id,
        plan_json: "{}".to_string(),
        join_strategy: JoinStrategy::Auto,
        schemas: vec![],
        frontier: 0,
        storage_root: "/tmp/storage".to_string(),
        sink_operator_id: OperatorId(10),
        output_columns: vec!["val".to_string()],
        primary_key: vec![0],
        merge_key_columns: vec![0],
        routing_columns: BTreeMap::from([("src".to_string(), 0)]),
    };

    let wire_deploy =
        serde_json::to_string(&WorkerMessage::DeployWorkload(deploy_req)).unwrap() + "\n";
    client_write
        .write_all(wire_deploy.as_bytes())
        .await
        .unwrap();

    // Worker receives ShardAssigned then Deploy
    line.clear();
    worker_reader.read_line(&mut line).await.unwrap();
    let shard_resp: ControlMessage = serde_json::from_str(&line).unwrap();
    assert!(matches!(shard_resp, ControlMessage::ShardAssigned { .. }));

    line.clear();
    worker_reader.read_line(&mut line).await.unwrap();
    let deploy_msg: ControlMessage = serde_json::from_str(&line).unwrap();
    let descriptor = match deploy_msg {
        ControlMessage::Deploy { descriptor } => descriptor,
        other => panic!("Expected Deploy, got {other:?}"),
    };
    let assigned_shard_id = descriptor.shard.shard_id;
    let assigned_lease_token = descriptor.shard.lease_token;

    // Worker sends DeploymentReady
    let ready_msg = WorkerMessage::DeploymentReady {
        version: 1,
        workload_id,
        shard_id: assigned_shard_id,
        worker_id: WorkerId(1),
        process_id: 9999,
        operator_ids: vec![OperatorId(10)],
        frontier: 1,
    };
    let wire_ready = serde_json::to_string(&ready_msg).unwrap() + "\n";
    worker_write.write_all(wire_ready.as_bytes()).await.unwrap();

    // Client receives DeploymentReady
    line.clear();
    client_reader.read_line(&mut line).await.unwrap();
    let resp: ControlMessage = serde_json::from_str(&line).unwrap();
    assert!(
        matches!(resp, ControlMessage::DeploymentReady { .. }),
        "Expected DeploymentReady, got {resp:?}"
    );

    // 3. Worker reports execution progress with rows in delta
    let progress_msg = WorkerMessage::ExecutionProgress {
        output: RuntimeOutputDelta {
            attempt_id: None,
            version: 1,
            request_id: "req-1".to_string(),
            workload_id,
            shard_id: assigned_shard_id,
            epoch: 1,
            operator_id: OperatorId(10),
            lease_token: assigned_lease_token,
            source: "src".to_string(),
            rows: vec![RuntimeRow {
                values_tsv: "42\tfoo".to_string(),
                weight: 1,
            }],
        },
        input_rows: 10,
        output_rows: 5,
    };
    let wire_prog = serde_json::to_string(&progress_msg).unwrap() + "\n";
    worker_write.write_all(wire_prog.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 4. Query ReadWorkload: Assert control plane retains ZERO row output deltas
    let read_msg = WorkerMessage::ReadWorkload { workload_id };
    let wire_read = serde_json::to_string(&read_msg).unwrap() + "\n";
    client_write.write_all(wire_read.as_bytes()).await.unwrap();
    line.clear();
    client_reader.read_line(&mut line).await.unwrap();
    let resp: ControlMessage = serde_json::from_str(&line).unwrap();
    if let ControlMessage::WorkloadSnapshot { snapshot } = resp {
        for shard in snapshot.shards {
            assert!(
                shard.deltas.is_empty(),
                "Control plane must NOT retain row payloads or output history! Found: {:?}",
                shard.deltas
            );
        }
        assert_eq!(snapshot.workers.len(), 1);
        assert_eq!(snapshot.workers[0].input_rows, 10);
        assert_eq!(snapshot.workers[0].output_rows, 5);
    } else {
        panic!("Expected WorkloadSnapshot, got {resp:?}");
    }

    // 5. Send SubmitSourceDelta with row payloads: the control plane routes the
    // payload directly to the owning worker without retaining it.
    let submit_with_rows = WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
        version: 1,
        request_id: "delta-1".to_string(),
        workload_id,
        epoch: 2,
        source: "src".to_string(),
        rows: vec![RuntimeRow {
            values_tsv: "100\tbar".to_string(),
            weight: 1,
        }],
    });
    let wire_submit = serde_json::to_string(&submit_with_rows).unwrap() + "\n";
    client_write
        .write_all(wire_submit.as_bytes())
        .await
        .unwrap();
    line.clear();
    worker_reader.read_line(&mut line).await.unwrap();
    let resp: ControlMessage = serde_json::from_str(&line).unwrap();
    let execute = match resp {
        ControlMessage::Execute { frame } => frame,
        other => panic!("Expected Execute, got {other:?}"),
    };
    assert_eq!(
        execute,
        RuntimeExchangeMessage {
            attempt_id: None,
            version: 1,
            request_id: "delta-1".to_string(),
            workload_id,
            shard_id: assigned_shard_id,
            epoch: 2,
            operator_id: OperatorId(10),
            lease_token: assigned_lease_token,
            source: "src".to_string(),
            rows: vec![RuntimeRow {
                values_tsv: "100\tbar".to_string(),
                weight: 1,
            }],
        }
    );

    let progress_msg = WorkerMessage::ExecutionProgress {
        output: RuntimeOutputDelta {
            attempt_id: None,
            version: 1,
            request_id: "delta-1".to_string(),
            workload_id,
            shard_id: assigned_shard_id,
            epoch: 2,
            operator_id: OperatorId(10),
            lease_token: assigned_lease_token,
            source: "src".to_string(),
            rows: vec![RuntimeRow {
                values_tsv: "100\tbar".to_string(),
                weight: 1,
            }],
        },
        input_rows: 1,
        output_rows: 1,
    };
    let wire_progress = serde_json::to_string(&progress_msg).unwrap() + "\n";
    worker_write
        .write_all(wire_progress.as_bytes())
        .await
        .unwrap();
    line.clear();
    client_reader.read_line(&mut line).await.unwrap();
    let resp: ControlMessage = serde_json::from_str(&line).unwrap();
    match resp {
        ControlMessage::SourceDeltaCommitted { request_id, epoch } => {
            assert_eq!(request_id, "delta-1");
            assert_eq!(epoch, 2);
        }
        other => panic!("Expected SourceDeltaCommitted, got {other:?}"),
    }

    // 6. Send SubmitSourceDelta with EMPTY rows: control acknowledges frontier metadata
    let submit_empty = WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
        version: 1,
        request_id: "delta-2".to_string(),
        workload_id,
        epoch: 2,
        source: "src".to_string(),
        rows: vec![],
    });
    let wire_empty = serde_json::to_string(&submit_empty).unwrap() + "\n";
    client_write.write_all(wire_empty.as_bytes()).await.unwrap();
    line.clear();
    client_reader.read_line(&mut line).await.unwrap();
    let resp: ControlMessage = serde_json::from_str(&line).unwrap();
    match resp {
        ControlMessage::SourceDeltaCommitted { request_id, epoch } => {
            assert_eq!(request_id, "delta-2");
            assert_eq!(epoch, 2);
        }
        other => panic!("Expected SourceDeltaCommitted, got {other:?}"),
    }

    handle.shutdown();
}

#[tokio::test]
async fn operator_stats_round_trip_is_scoped_to_workload_and_operator_ids() {
    let catalog = TopologyCatalog::new();
    let registration = WorkerRegistration::new(
        WorkerId(1),
        NodeRole::Worker,
        "127.0.0.1:9001",
        CapacityHeadroom::FULL,
    );
    catalog.register(&registration);
    let handle = ControlService::new(catalog)
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();

    let worker = TcpStream::connect(handle.addr).await.unwrap();
    let (worker_read, mut worker_write) = worker.into_split();
    let mut worker_reader = BufReader::new(worker_read);
    write_worker_message(&mut worker_write, &WorkerMessage::Register(registration)).await;
    assert!(matches!(
        read_control_message(&mut worker_reader).await,
        ControlMessage::Registered {
            worker_id: WorkerId(1)
        }
    ));
    assert!(matches!(
        read_control_message(&mut worker_reader).await,
        ControlMessage::TopologyChanged { .. }
    ));

    let gateway_addr = handle.addr.to_string();
    for (workload_id, sink_operator_id) in [
        (WorkloadId(42), OperatorId(400)),
        (WorkloadId(43), OperatorId(500)),
    ] {
        let deployment = DeploymentRequest {
            version: 1,
            workload_id,
            plan_json: "{}".to_string(),
            join_strategy: JoinStrategy::Auto,
            schemas: vec![],
            frontier: 0,
            storage_root: "/tmp/operator-stats".to_string(),
            sink_operator_id,
            output_columns: vec!["value".to_string()],
            primary_key: vec![0],
            merge_key_columns: vec![0],
            routing_columns: BTreeMap::from([("source".to_string(), 0)]),
        };
        let deploy_task = tokio::spawn({
            let gateway_addr = gateway_addr.clone();
            async move {
                rockstream_runtime::data_plane::DataPlaneClient::new(gateway_addr)
                    .deploy(deployment)
                    .await
            }
        });
        let descriptor = loop {
            match read_control_message(&mut worker_reader).await {
                ControlMessage::Deploy { descriptor } => break descriptor,
                ControlMessage::ShardAssigned { .. } => {}
                other => panic!("unexpected worker command during deployment: {other:?}"),
            }
        };
        write_worker_message(
            &mut worker_write,
            &WorkerMessage::DeploymentReady {
                version: descriptor.version,
                workload_id,
                shard_id: descriptor.shard.shard_id,
                worker_id: WorkerId(1),
                process_id: 1,
                operator_ids: vec![descriptor.sink_operator_id],
                frontier: descriptor.frontier,
            },
        )
        .await;
        deploy_task.await.unwrap().unwrap();
    }

    for (workload_id, requested_ids, own_rows, other_view_id) in [
        (
            WorkloadId(42),
            [OperatorId(101), OperatorId(102)],
            [1.0, 2.0],
            OperatorId(201),
        ),
        (
            WorkloadId(43),
            [OperatorId(201), OperatorId(202)],
            [3.0, 4.0],
            OperatorId(101),
        ),
    ] {
        let stats_task = tokio::spawn({
            let gateway_addr = gateway_addr.clone();
            async move {
                rockstream_runtime::data_plane::DataPlaneClient::new(gateway_addr)
                    .read_operator_stats(workload_id, requested_ids.to_vec())
                    .await
            }
        });
        let (request_id, requested) = match read_control_message(&mut worker_reader).await {
            ControlMessage::ReadOperatorStats {
                request_id,
                workload_id: actual_workload_id,
                operator_ids,
            } => {
                assert_eq!(actual_workload_id, workload_id);
                (request_id, operator_ids.into_iter().collect::<HashSet<_>>())
            }
            other => panic!("expected operator stats request, got {other:?}"),
        };
        assert_eq!(requested, HashSet::from(requested_ids));

        let mut stats: Vec<IdentifiedOperatorStats> = requested_ids
            .into_iter()
            .zip(own_rows)
            .map(|(operator_id, rows_per_s)| IdentifiedOperatorStats {
                operator_id,
                stats: OperatorStats {
                    rows_per_s,
                    state_reads: rows_per_s as u64,
                    rmw_ratio: 0.0,
                    p99_latency_ms: 1.0,
                    dlq_entries: 0,
                },
                rmw_avoided: 0,
                rmw_required: 0,
            })
            .collect();
        stats.reverse();
        stats.push(IdentifiedOperatorStats {
            operator_id: other_view_id,
            stats: OperatorStats {
                rows_per_s: 99.0,
                state_reads: 99,
                rmw_ratio: 0.0,
                p99_latency_ms: 99.0,
                dlq_entries: 99,
            },
            rmw_avoided: 0,
            rmw_required: 0,
        });
        write_worker_message(
            &mut worker_write,
            &WorkerMessage::OperatorStatsReport {
                request_id,
                workload_id,
                worker_id: WorkerId(1),
                stats,
            },
        )
        .await;

        let stats = stats_task.await.unwrap().unwrap().unwrap();
        assert_eq!(
            stats
                .iter()
                .map(|stat| stat.operator_id)
                .collect::<HashSet<_>>(),
            HashSet::from(requested_ids)
        );
        for ((operator_id, expected_rows), stat) in
            requested_ids.into_iter().zip(own_rows).zip(stats.iter())
        {
            assert_eq!(stat.operator_id, operator_id);
            assert_eq!(stat.stats.rows_per_s, expected_rows);
        }
    }

    handle.shutdown();
}
