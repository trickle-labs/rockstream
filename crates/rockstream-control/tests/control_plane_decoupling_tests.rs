//! Control Plane Boundary Decoupling Tests (v0.67 Slice 1 / Phase 3a).
//!
//! Asserts that the control plane never buffers or stores row payloads or query
//! output history; it forwards routed frames without retaining their contents.

use std::collections::BTreeMap;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use rockstream_control::service::ControlService;
use rockstream_control::shard::ShardManager;
use rockstream_control::topology::TopologyCatalog;
use rockstream_types::config::JoinStrategy;
use rockstream_types::data_plane::{
    DeploymentRequest, RuntimeExchangeMessage, RuntimeOutputDelta, RuntimeRow, SourceDeltaRequest,
};
use rockstream_types::ids::{OperatorId, WorkerId, WorkloadId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};

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

    let handle = service.start("127.0.0.1:0").await.unwrap();
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
