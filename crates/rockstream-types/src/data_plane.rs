//! Wire types for process-bound worker execution.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{LeaseToken, OperatorId, ShardId, WorkerId, WorkloadId};
use crate::lease::ShardLease;
use crate::timestamp::Epoch;

pub const DEPLOYMENT_DESCRIPTOR_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentSchema {
    pub relation: String,
    pub columns: Vec<DeploymentColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentRequest {
    pub version: u32,
    pub workload_id: WorkloadId,
    pub plan_json: String,
    pub schemas: Vec<DeploymentSchema>,
    pub frontier: Epoch,
    pub storage_root: String,
    pub sink_operator_id: OperatorId,
    pub output_columns: Vec<String>,
    pub primary_key: Vec<usize>,
    pub merge_key_columns: Vec<usize>,
    pub routing_columns: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentDescriptor {
    pub version: u32,
    pub workload_id: WorkloadId,
    pub plan_json: String,
    pub schemas: Vec<DeploymentSchema>,
    pub frontier: Epoch,
    pub storage_root: String,
    pub sink_operator_id: OperatorId,
    pub output_columns: Vec<String>,
    pub primary_key: Vec<usize>,
    pub merge_key_columns: Vec<usize>,
    pub routing_columns: BTreeMap<String, usize>,
    pub shard: ShardLease,
    pub storage_identity: String,
}

impl DeploymentDescriptor {
    pub fn new(request: DeploymentRequest, shard: ShardLease, storage_identity: String) -> Self {
        Self {
            version: request.version,
            workload_id: request.workload_id,
            plan_json: request.plan_json,
            schemas: request.schemas,
            frontier: request.frontier,
            storage_root: request.storage_root,
            sink_operator_id: request.sink_operator_id,
            output_columns: request.output_columns,
            primary_key: request.primary_key,
            merge_key_columns: request.merge_key_columns,
            routing_columns: request.routing_columns,
            shard,
            storage_identity,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeRow {
    pub values_tsv: String,
    pub weight: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceDeltaRequest {
    pub version: u32,
    pub request_id: String,
    pub workload_id: WorkloadId,
    pub epoch: Epoch,
    pub source: String,
    pub rows: Vec<RuntimeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeExchangeMessage {
    pub version: u32,
    pub request_id: String,
    pub workload_id: WorkloadId,
    pub shard_id: ShardId,
    pub epoch: Epoch,
    pub operator_id: OperatorId,
    pub lease_token: LeaseToken,
    pub source: String,
    pub rows: Vec<RuntimeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeOutputDelta {
    pub version: u32,
    pub request_id: String,
    pub workload_id: WorkloadId,
    pub shard_id: ShardId,
    pub epoch: Epoch,
    pub operator_id: OperatorId,
    pub lease_token: LeaseToken,
    pub source: String,
    pub rows: Vec<RuntimeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerExecutionStatus {
    pub worker_id: WorkerId,
    pub process_id: u32,
    pub shard_ids: Vec<ShardId>,
    pub input_rows: u64,
    pub output_rows: u64,
    pub frontier: Epoch,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardOutput {
    pub shard_id: ShardId,
    pub deltas: Vec<RuntimeOutputDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadSnapshot {
    pub deployment: DeploymentRequest,
    pub shards: Vec<ShardOutput>,
    pub workers: Vec<WorkerExecutionStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{ControlMessage, WorkerMessage};

    fn request() -> DeploymentRequest {
        DeploymentRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            workload_id: WorkloadId(7),
            plan_json: "{\"operator\":\"aggregate\"}".into(),
            schemas: vec![DeploymentSchema {
                relation: "orders".into(),
                columns: vec![DeploymentColumn {
                    name: "amount".into(),
                    data_type: "i64".into(),
                }],
            }],
            frontier: 11,
            storage_root: "/tmp/rockstream".into(),
            sink_operator_id: OperatorId(3),
            output_columns: vec!["amount".into()],
            primary_key: vec![0],
            merge_key_columns: vec![0],
            routing_columns: BTreeMap::from([("orders".into(), 0)]),
        }
    }

    #[test]
    fn data_plane_wire_types_round_trip_exactly() {
        let request = request();
        let deployment = DeploymentDescriptor::new(
            request.clone(),
            ShardLease::new(ShardId(2), WorkerId(5), LeaseToken(13)),
            "lfs:worker-5/shard-2".into(),
        );
        let output = RuntimeOutputDelta {
            version: 1,
            request_id: "request-9".into(),
            workload_id: WorkloadId(7),
            shard_id: ShardId(2),
            epoch: 12,
            operator_id: OperatorId(3),
            lease_token: LeaseToken(13),
            source: "orders".into(),
            rows: vec![RuntimeRow {
                values_tsv: "42".into(),
                weight: -1,
            }],
        };
        let snapshot = WorkloadSnapshot {
            deployment: request.clone(),
            shards: vec![ShardOutput {
                shard_id: ShardId(2),
                deltas: vec![output.clone()],
            }],
            workers: vec![WorkerExecutionStatus {
                worker_id: WorkerId(5),
                process_id: 1234,
                shard_ids: vec![ShardId(2)],
                input_rows: 8,
                output_rows: 4,
                frontier: 12,
                ready: true,
            }],
        };
        let source = SourceDeltaRequest {
            version: 1,
            request_id: "request-9".into(),
            workload_id: WorkloadId(7),
            epoch: 12,
            source: "orders".into(),
            rows: output.rows.clone(),
        };
        let exchange = RuntimeExchangeMessage {
            version: output.version,
            request_id: output.request_id.clone(),
            workload_id: output.workload_id,
            shard_id: output.shard_id,
            epoch: output.epoch,
            operator_id: output.operator_id,
            lease_token: output.lease_token,
            source: output.source.clone(),
            rows: output.rows.clone(),
        };

        macro_rules! assert_round_trip {
            ($value:expr, $type:ty) => {
                assert_eq!(
                    serde_json::from_str::<$type>(&serde_json::to_string(&$value).unwrap())
                        .unwrap(),
                    $value
                );
            };
        }
        assert_round_trip!(request, DeploymentRequest);
        assert_round_trip!(deployment, DeploymentDescriptor);
        assert_round_trip!(source, SourceDeltaRequest);
        assert_round_trip!(exchange, RuntimeExchangeMessage);
        assert_round_trip!(output, RuntimeOutputDelta);
        assert_round_trip!(snapshot, WorkloadSnapshot);
    }

    #[test]
    fn topology_data_plane_messages_round_trip_exactly() {
        let request = request();
        let descriptor = DeploymentDescriptor::new(
            request.clone(),
            ShardLease::new(ShardId(2), WorkerId(5), LeaseToken(13)),
            "lfs:worker-5/shard-2".into(),
        );
        let rows = vec![RuntimeRow {
            values_tsv: "42".into(),
            weight: 1,
        }];
        let output = RuntimeOutputDelta {
            version: 1,
            request_id: "request-9".into(),
            workload_id: WorkloadId(7),
            shard_id: ShardId(2),
            epoch: 12,
            operator_id: OperatorId(3),
            lease_token: LeaseToken(13),
            source: "orders".into(),
            rows: rows.clone(),
        };
        let status = WorkerExecutionStatus {
            worker_id: WorkerId(5),
            process_id: 1234,
            shard_ids: vec![ShardId(2)],
            input_rows: 8,
            output_rows: 4,
            frontier: 12,
            ready: true,
        };
        let snapshot = WorkloadSnapshot {
            deployment: request.clone(),
            shards: vec![ShardOutput {
                shard_id: ShardId(2),
                deltas: vec![output.clone()],
            }],
            workers: vec![status.clone()],
        };
        let frame = RuntimeExchangeMessage {
            version: 1,
            request_id: "request-9".into(),
            workload_id: WorkloadId(7),
            shard_id: ShardId(2),
            epoch: 12,
            operator_id: OperatorId(3),
            lease_token: LeaseToken(13),
            source: "orders".into(),
            rows,
        };
        let control = vec![
            ControlMessage::Deploy { descriptor },
            ControlMessage::Execute { frame },
            ControlMessage::DeploymentReady {
                workload_id: WorkloadId(7),
                workers: vec![status],
            },
            ControlMessage::SourceDeltaCommitted {
                request_id: "request-9".into(),
                epoch: 12,
            },
            ControlMessage::WorkloadSnapshot { snapshot },
        ];
        let worker = vec![
            WorkerMessage::DeployWorkload(request.clone()),
            WorkerMessage::DeploymentReady {
                version: 1,
                workload_id: WorkloadId(7),
                shard_id: ShardId(2),
                worker_id: WorkerId(5),
                process_id: 1234,
                operator_ids: vec![OperatorId(1), OperatorId(3)],
                frontier: 12,
            },
            WorkerMessage::SubmitSourceDelta(SourceDeltaRequest {
                version: 1,
                request_id: "request-9".into(),
                workload_id: WorkloadId(7),
                epoch: 12,
                source: "orders".into(),
                rows: vec![RuntimeRow {
                    values_tsv: "42".into(),
                    weight: 1,
                }],
            }),
            WorkerMessage::ExecutionProgress {
                output,
                input_rows: 8,
                output_rows: 4,
            },
            WorkerMessage::ReadWorkload {
                workload_id: WorkloadId(7),
            },
        ];

        let control_json = serde_json::to_string(&control).unwrap();
        assert_eq!(
            serde_json::to_string(
                &serde_json::from_str::<Vec<ControlMessage>>(&control_json).unwrap()
            )
            .unwrap(),
            control_json
        );
        let worker_json = serde_json::to_string(&worker).unwrap();
        assert_eq!(
            serde_json::to_string(
                &serde_json::from_str::<Vec<WorkerMessage>>(&worker_json).unwrap()
            )
            .unwrap(),
            worker_json
        );
    }
}
