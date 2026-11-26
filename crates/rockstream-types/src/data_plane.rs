//! Wire types for process-bound worker execution.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::config::JoinStrategy;
use crate::ids::{LeaseToken, OperatorId, ShardId, WorkerId, WorkloadId};
use crate::lease::ShardLease;
use crate::timestamp::Epoch;

pub const DEPLOYMENT_DESCRIPTOR_VERSION: u32 = 2;

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
    pub namespace: String,
    pub workload_name: String,
    pub plan_json: String,
    pub join_strategy: JoinStrategy,
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
    pub join_strategy: JoinStrategy,
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
    /// Logical workload-relative shard index (`0..shard_count`), independent of
    /// the globally allocated [`ShardId`].
    #[serde(default)]
    pub shard_index: u32,
}

impl DeploymentDescriptor {
    pub fn new(request: DeploymentRequest, shard: ShardLease, storage_identity: String) -> Self {
        Self::with_shard_index(request, shard, storage_identity, 0)
    }

    pub fn with_shard_index(
        request: DeploymentRequest,
        shard: ShardLease,
        storage_identity: String,
        shard_index: u32,
    ) -> Self {
        Self {
            version: request.version,
            workload_id: request.workload_id,
            plan_json: request.plan_json,
            join_strategy: request.join_strategy,
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
            shard_index,
        }
    }
}

/// Placement metadata for one deployed shard of a workload, used by direct
/// data-plane routers to partition and stream Arrow IPC batches straight to
/// shard owners without routing row payloads through `ControlService`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadShardPlacement {
    pub shard_id: ShardId,
    pub shard_index: u32,
    pub worker_id: WorkerId,
    pub worker_address: String,
    pub lease_token: LeaseToken,
    pub operator_id: OperatorId,
    pub storage_identity: String,
    pub frontier: Epoch,
}

/// Cached, versioned placement table for a deployed workload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkloadTopologySnapshot {
    pub topology_epoch: u64,
    pub workload_id: WorkloadId,
    /// The deployed logical plan, used for executable skew admission.
    #[serde(default)]
    pub plan_json: String,
    pub routing_columns: BTreeMap<String, usize>,
    pub schemas: Vec<DeploymentSchema>,
    pub shards: Vec<WorkloadShardPlacement>,
    pub workers: Vec<crate::topology::WorkerInfo>,
}

/// Explicit single-owner lease for a source connector task so multiple gateway
/// replicas never duplicate ingestion for the same connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorLease {
    pub connector_id: crate::ids::ConnectorId,
    pub owner_id: String,
    pub lease_token: LeaseToken,
    pub acquired_at_ms: u64,
    pub expires_at_ms: u64,
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

/// Durable record of a source epoch that has not yet been fully reconciled.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnedEpochIntent {
    pub attempt_id: uuid::Uuid,
    pub version: u32,
    pub request_id: String,
    pub workload_id: WorkloadId,
    pub epoch: Epoch,
    pub source: String,
    pub anchor_shard: ShardId,
    pub target_shards: Vec<ShardId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JoinBootstrap {
    /// The opposite input whose committed low-cardinality rows must be
    /// installed before this frame's high-cardinality delta is evaluated.
    pub source: String,
    pub rows: Vec<RuntimeRow>,
    /// Exact final group transitions when executable arithmetic admission
    /// returns a key to its canonical summary owner. These are part of the
    /// same replay identity and durable sink commit as the source epoch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub final_rows: Vec<RuntimeRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeExchangeMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<uuid::Uuid>,
    pub version: u32,
    pub request_id: String,
    pub workload_id: WorkloadId,
    pub shard_id: ShardId,
    pub epoch: Epoch,
    pub operator_id: OperatorId,
    pub lease_token: LeaseToken,
    pub source: String,
    pub rows: Vec<RuntimeRow>,
    /// A bounded low-side snapshot installed atomically with the source
    /// delta, arrangement state, output, and execution replay identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_bootstrap: Option<JoinBootstrap>,
}

impl RuntimeExchangeMessage {
    pub fn encoded_len(&self) -> Result<usize, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| bytes.len())
    }

    pub fn record_encoded_exchange(
        &self,
        worker_id: WorkerId,
        strategy: crate::metrics::R1ExecutionStrategy,
    ) -> Result<u64, serde_json::Error> {
        let bytes = self.encoded_len()? as u64;
        crate::metrics::record_r1_execution(
            crate::metrics::R1ExecutionKey {
                worker_id,
                workload_id: self.workload_id,
                shard_id: self.shard_id,
                operator_id: self.operator_id,
                strategy,
            },
            crate::metrics::R1ExecutionCounters {
                encoded_exchange_bytes: bytes,
                ..Default::default()
            },
        );
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeOutputDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<uuid::Uuid>,
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
    #[serde(default)]
    pub lease_token: Option<LeaseToken>,
    /// Durable application frontier captured under the shard execution gate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_epoch: Option<Epoch>,
    /// Completed global routing epoch; present only on the workload anchor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_epoch: Option<Epoch>,
    #[serde(default)]
    pub deltas: Vec<RuntimeOutputDelta>,
    #[serde(default)]
    pub rows: Vec<RuntimeRow>,
}

pub const WORKLOAD_SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadSnapshot {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub complete: bool,
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
            namespace: "public".into(),
            workload_name: "orders_aggregate".into(),
            plan_json: "{\"operator\":\"aggregate\"}".into(),
            join_strategy: JoinStrategy::Auto,
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
            attempt_id: None,
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
            version: WORKLOAD_SNAPSHOT_VERSION,
            complete: true,
            deployment: request.clone(),
            shards: vec![ShardOutput {
                shard_id: ShardId(2),
                lease_token: Some(LeaseToken(13)),
                committed_epoch: None,
                routing_epoch: None,
                deltas: vec![output.clone()],
                rows: vec![],
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
            join_bootstrap: None,
            attempt_id: None,
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
        let encoded_exchange = "{\"version\":1,\"request_id\":\"request-9\",\"workload_id\":7,\"shard_id\":2,\"epoch\":12,\"operator_id\":3,\"lease_token\":13,\"source\":\"orders\",\"rows\":[{\"values_tsv\":\"42\",\"weight\":-1}]}";
        assert_eq!(serde_json::to_string(&exchange).unwrap(), encoded_exchange);
        assert_eq!(exchange.encoded_len().unwrap(), encoded_exchange.len());

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
            attempt_id: None,
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
            version: WORKLOAD_SNAPSHOT_VERSION,
            complete: true,
            deployment: request.clone(),
            shards: vec![ShardOutput {
                shard_id: ShardId(2),
                lease_token: Some(LeaseToken(13)),
                committed_epoch: None,
                routing_epoch: None,
                deltas: vec![output.clone()],
                rows: vec![],
            }],
            workers: vec![status.clone()],
        };
        let frame = RuntimeExchangeMessage {
            join_bootstrap: None,
            attempt_id: None,
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
            ControlMessage::ReadWorkload {
                version: WORKLOAD_SNAPSHOT_VERSION,
                request_id: "read-9".into(),
                workload_id: WorkloadId(7),
                shard_ids: vec![ShardId(2)],
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
                version: WORKLOAD_SNAPSHOT_VERSION,
                workload_id: WorkloadId(7),
            },
            WorkerMessage::WorkloadReadReport {
                version: WORKLOAD_SNAPSHOT_VERSION,
                request_id: "read-9".into(),
                workload_id: WorkloadId(7),
                worker_id: WorkerId(5),
                available: true,
                shards: vec![ShardOutput {
                    shard_id: ShardId(2),
                    lease_token: Some(LeaseToken(13)),
                    committed_epoch: None,
                    routing_epoch: None,
                    deltas: vec![],
                    rows: vec![RuntimeRow {
                        values_tsv: "42".into(),
                        weight: 2,
                    }],
                }],
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
