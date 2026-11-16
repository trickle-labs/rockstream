use std::collections::BTreeMap;
use std::time::Duration;

use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_types::config::JoinStrategy;
use rockstream_types::data_plane::{
    DeploymentRequest, OwnedEpochIntent, RuntimeOutputDelta, RuntimeRow, SourceDeltaRequest,
};
use rockstream_types::ids::{OperatorId, ShardId, WorkerId, WorkloadId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

async fn send<W: AsyncWrite + Unpin>(writer: &mut W, message: &WorkerMessage) {
    writer
        .write_all((serde_json::to_string(message).unwrap() + "\n").as_bytes())
        .await
        .unwrap();
}

async fn receive<R: AsyncBufRead + Unpin>(reader: &mut R) -> ControlMessage {
    let mut line = String::new();
    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn owned_epoch_is_prepared_before_dispatch_and_finished_before_reply() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        let control_service = ControlService::new(catalog.clone())
            .with_shard_manager(manager.clone())
            .with_insecure_dev_mode();
        let control_handle = control_service.start("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(control_handle.addr).await.unwrap();
        let (read, mut worker_write) = stream.into_split();
        let mut worker_read = BufReader::new(read);
        let worker_id = WorkerId(1);
        send(
            &mut worker_write,
            &WorkerMessage::Register(WorkerRegistration::new(
                worker_id,
                NodeRole::Worker,
                "127.0.0.1:9001".to_owned(),
                CapacityHeadroom::FULL,
            )),
        )
        .await;
        loop {
            if let ControlMessage::Registered { worker_id: actual } =
                receive(&mut worker_read).await
            {
                assert_eq!(actual, worker_id);
                break;
            }
        }
        send(
            &mut worker_write,
            &WorkerMessage::OwnedShuffleReady { worker_id },
        )
        .await;

        let stream = TcpStream::connect(control_handle.addr).await.unwrap();
        let (read, mut client_write) = stream.into_split();
        let mut client_read = BufReader::new(read);
        let workload_id = WorkloadId(9);
        let request = DeploymentRequest {
            version: 1,
            workload_id,
            plan_json: "{}".to_owned(),
            join_strategy: JoinStrategy::Auto,
            schemas: vec![],
            frontier: 0,
            storage_root: "/tmp/owned-epoch".to_owned(),
            sink_operator_id: OperatorId(10),
            output_columns: vec!["value".to_owned()],
            primary_key: vec![0],
            merge_key_columns: vec![],
            routing_columns: BTreeMap::from([("items".to_owned(), 0)]),
        };
        send(&mut client_write, &WorkerMessage::DeployWorkload(request)).await;
        let descriptor = loop {
            if let ControlMessage::Deploy { descriptor } = receive(&mut worker_read).await {
                break descriptor;
            }
        };
        send(
            &mut worker_write,
            &WorkerMessage::DeploymentReady {
                version: 1,
                workload_id,
                shard_id: descriptor.shard.shard_id,
                worker_id,
                process_id: 1,
                operator_ids: vec![OperatorId(10)],
                frontier: 0,
            },
        )
        .await;
        match receive(&mut client_read).await {
            ControlMessage::DeploymentReady {
                workload_id: actual,
                ..
            } => assert_eq!(actual, workload_id),
            other => panic!("unexpected deployment response: {other:?}"),
        }

        let rows = vec![RuntimeRow {
            values_tsv: "1".to_owned(),
            weight: 1,
        }];
        let request_id = "owned-order".to_owned();
        send(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: request_id.clone(),
                workload_id,
                epoch: 1,
                source: "items".to_owned(),
                rows: rows.clone(),
            }),
        )
        .await;
        let intent = match receive(&mut worker_read).await {
            ControlMessage::PrepareOwnedEpoch { intent } => intent,
            other => panic!("expected preparation, got {other:?}"),
        };
        assert_eq!(
            intent,
            OwnedEpochIntent {
                attempt_id: intent.attempt_id,
                version: 1,
                request_id: request_id.clone(),
                workload_id,
                epoch: 1,
                source: "items".to_owned(),
                anchor_shard: descriptor.shard.shard_id,
                target_shards: vec![descriptor.shard.shard_id],
            }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receive(&mut worker_read))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receive(&mut client_read))
                .await
                .is_err()
        );
        send(
            &mut worker_write,
            &WorkerMessage::OwnedEpochPrepared {
                worker_id,
                intent: intent.clone(),
                error: None,
            },
        )
        .await;
        let frame = match receive(&mut worker_read).await {
            ControlMessage::DispatchOwnedShuffle { target, frame } => {
                assert_eq!(target.worker_id, worker_id);
                frame
            }
            other => panic!("expected dispatch, got {other:?}"),
        };
        assert_eq!(
            (
                frame.request_id.clone(),
                frame.workload_id,
                frame.shard_id,
                frame.epoch,
                frame.source.clone(),
                frame.rows.clone()
            ),
            (
                request_id.clone(),
                workload_id,
                descriptor.shard.shard_id,
                1,
                "items".to_owned(),
                rows
            )
        );
        let output: RuntimeOutputDelta =
            serde_json::from_value(serde_json::to_value(&frame).unwrap()).unwrap();
        send(
            &mut worker_write,
            &WorkerMessage::ExecutionProgress {
                output: output.clone(),
                input_rows: 1,
                output_rows: 1,
            },
        )
        .await;
        match receive(&mut worker_read).await {
            ControlMessage::FinishOwnedEpoch { intent: finished } => assert_eq!(finished, intent),
            other => panic!("expected finish, got {other:?}"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receive(&mut client_read))
                .await
                .is_err()
        );
        send(
            &mut worker_write,
            &WorkerMessage::OwnedEpochFinished {
                worker_id,
                intent: intent.clone(),
                error: None,
            },
        )
        .await;
        match receive(&mut client_read).await {
            ControlMessage::SourceDeltaCommitted {
                request_id: actual,
                epoch,
            } => assert_eq!((actual, epoch), (request_id.clone(), 1)),
            other => panic!("unexpected completion: {other:?}"),
        }

        let next_rows = vec![RuntimeRow {
            values_tsv: "2".to_owned(),
            weight: 1,
        }];
        send(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: request_id.clone(),
                workload_id,
                epoch: 1,
                source: "items".to_owned(),
                rows: next_rows.clone(),
            }),
        )
        .await;
        let next_intent = match receive(&mut worker_read).await {
            ControlMessage::PrepareOwnedEpoch { intent } => intent,
            other => panic!("expected replay preparation, got {other:?}"),
        };
        assert_ne!(intent.attempt_id, next_intent.attempt_id);
        send(
            &mut worker_write,
            &WorkerMessage::OwnedEpochPrepared {
                worker_id,
                intent: next_intent.clone(),
                error: None,
            },
        )
        .await;
        let next_frame = match receive(&mut worker_read).await {
            ControlMessage::DispatchOwnedShuffle { frame, .. } => frame,
            other => panic!("expected replay dispatch, got {other:?}"),
        };
        assert_eq!(next_frame.rows, next_rows);
        send(
            &mut worker_write,
            &WorkerMessage::ExecutionProgress {
                output,
                input_rows: 1,
                output_rows: 1,
            },
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receive(&mut worker_read))
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receive(&mut client_read))
                .await
                .is_err()
        );
        let next_output: RuntimeOutputDelta =
            serde_json::from_value(serde_json::to_value(&next_frame).unwrap()).unwrap();
        send(
            &mut worker_write,
            &WorkerMessage::ExecutionProgress {
                output: next_output,
                input_rows: 1,
                output_rows: 1,
            },
        )
        .await;
        match receive(&mut worker_read).await {
            ControlMessage::FinishOwnedEpoch { intent: finished } => {
                assert_eq!(finished, next_intent)
            }
            other => panic!("expected replay finish, got {other:?}"),
        }
        send(
            &mut worker_write,
            &WorkerMessage::OwnedEpochFinished {
                worker_id,
                intent: next_intent,
                error: None,
            },
        )
        .await;
        match receive(&mut client_read).await {
            ControlMessage::SourceDeltaCommitted {
                request_id: actual,
                epoch,
            } => assert_eq!((actual, epoch), (request_id, 1)),
            other => panic!("unexpected replay completion: {other:?}"),
        }

        send(
            &mut client_write,
            &WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: "prepare-failure".to_owned(),
                workload_id,
                epoch: 2,
                source: "items".to_owned(),
                rows: vec![RuntimeRow {
                    values_tsv: "2".to_owned(),
                    weight: 1,
                }],
            }),
        )
        .await;
        let intent = match receive(&mut worker_read).await {
            ControlMessage::PrepareOwnedEpoch { intent } => intent,
            other => panic!("expected second preparation, got {other:?}"),
        };
        assert_eq!(
            (
                intent.request_id.as_str(),
                intent.epoch,
                intent.anchor_shard
            ),
            ("prepare-failure", 2, ShardId(144))
        );
        send(
            &mut worker_write,
            &WorkerMessage::OwnedEpochPrepared {
                worker_id,
                intent,
                error: Some("test failure".to_owned()),
            },
        )
        .await;
        match receive(&mut client_read).await {
            ControlMessage::OperationFailed {
                code,
                message,
                next_steps,
            } => assert_eq!(
                (code, message, next_steps),
                (
                    "RS-0001".to_owned(),
                    "owned epoch preparation failed: test failure".to_owned(),
                    "Preserve the source input and shard storage; automatic recovery is unavailable. Contact the operator before retrying.".to_owned()
                )
            ),
            other => panic!("unexpected preparation response: {other:?}"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), receive(&mut worker_read))
                .await
                .is_err()
        );
        control_handle.shutdown();
    })
    .await
    .expect("owned epoch fixture timed out");
}
