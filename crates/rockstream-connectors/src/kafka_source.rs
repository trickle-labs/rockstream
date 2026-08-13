//! Kafka source connector backed by a real consumer group (§13.3).

use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};
use serde::Deserialize;

use crate::source_connector::{PollDeltaResult, SnapshotStream, SourceConnector, SourceError};
use crate::source_epoch::{OffsetToken, SnapshotDeltaFence};
use crate::source_json::{json_rows_to_batch, JsonRow};

/// Native Kafka queue bound in KiB; one overflow record is retained locally.
pub const KAFKA_SOURCE_BUFFER_LIMIT: usize = 50_000;

#[derive(Debug, Clone)]
struct KafkaRecord {
    offset: u64,
    partition: i32,
    timestamp: i64,
    values: JsonRow,
    weight: i64,
    bytes: usize,
}

#[derive(Deserialize)]
struct KafkaPayload {
    timestamp: i64,
    values: JsonRow,
    #[serde(default = "default_weight")]
    weight: i64,
}

const fn default_weight() -> i64 {
    1
}

/// Kafka source using a real `rdkafka::consumer::StreamConsumer`.
pub struct KafkaSource {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    consumer: StreamConsumer,
    runtime: Option<tokio::runtime::Runtime>,
    topic: String,
    paused: bool,
    watermarks: BTreeMap<i32, i64>,
    pending_record: Option<KafkaRecord>,
    last_poll_fill_level: usize,
    last_polled: Option<OffsetToken>,
    last_committed: Option<(Epoch, OffsetToken)>,
}

impl KafkaSource {
    /// Connect and subscribe this source to a Kafka consumer group.
    pub fn connect(
        connector_id: ConnectorId,
        schema: SchemaRef,
        bootstrap_servers: &str,
        topic: &str,
        group_id: &str,
    ) -> Result<Self, SourceError> {
        if bootstrap_servers.is_empty() || topic.is_empty() || group_id.is_empty() {
            return Err(SourceError::Io(
                "Kafka configuration is incomplete. Next steps: provide bootstrap servers, topic, and group id"
                    .to_string(),
            ));
        }

        let runtime = if tokio::runtime::Handle::try_current().is_err() {
            Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .map_err(|error| {
                        SourceError::Io(format!(
                        "Kafka runtime creation failed: {error}. Next steps: retry source startup"
                    ))
                    })?,
            )
        } else {
            None
        };
        let config = || {
            ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("group.id", group_id)
                .set("enable.auto.commit", "false")
                .set("enable.auto.offset.store", "false")
                .set("auto.offset.reset", "earliest")
                .set(
                    "queued.max.messages.kbytes",
                    KAFKA_SOURCE_BUFFER_LIMIT.to_string(),
                )
                .create::<StreamConsumer>()
        };
        let consumer = if let Some(runtime) = &runtime {
            let _guard = runtime.enter();
            config()
        } else {
            config()
        }
        .map_err(|error| SourceError::Io(format!(
            "Kafka consumer creation failed: {error}. Next steps: verify broker connectivity and consumer configuration"
        )))?;
        consumer.subscribe(&[topic]).map_err(|error| {
            SourceError::Io(format!(
                "Kafka subscription failed: {error}. Next steps: verify that topic {topic:?} exists and is authorized"
            ))
        })?;

        Ok(Self {
            _connector_id: connector_id,
            schema,
            consumer,
            runtime,
            topic: topic.to_owned(),
            paused: false,
            watermarks: BTreeMap::new(),
            pending_record: None,
            last_poll_fill_level: 0,
            last_polled: None,
            last_committed: None,
        })
    }

    /// The number of partitions currently assigned by the Kafka group.
    pub fn assigned_partition_count(&self) -> usize {
        self.watermarks.len()
    }

    /// Bounded local-buffer fill level (zero or one overflow record).
    pub fn last_poll_fill_level(&self) -> usize {
        self.last_poll_fill_level
    }

    /// Retrieve a partition's next offset from a serialized `OffsetToken`.
    pub fn get_partition_offset(&self, token: &OffsetToken, partition_id: u64) -> Option<u64> {
        if token.as_bytes().is_empty() {
            return Some(0);
        }
        let map: BTreeMap<u64, u64> = serde_json::from_slice(token.as_bytes()).ok()?;
        Some(map.get(&partition_id).copied().unwrap_or(0))
    }

    fn current_global_watermark(&self) -> Option<EventTimeWatermark> {
        self.watermarks
            .values()
            .copied()
            .min()
            .filter(|watermark| *watermark != i64::MIN)
            .map(|watermark| watermark as u64)
    }

    fn refresh_assignment(&mut self) -> Result<BTreeSet<i32>, SourceError> {
        let assigned = self
            .consumer
            .assignment()
            .map_err(|error| SourceError::PollDeltaFailed {
                reason: format!(
                    "Kafka assignment lookup failed: {error}. Next steps: retry after consumer-group rebalance"
                ),
            })?
            .elements_for_topic(&self.topic)
            .into_iter()
            .map(|partition| partition.partition())
            .collect::<BTreeSet<_>>();
        self.watermarks
            .retain(|partition, _| assigned.contains(partition));
        for partition in &assigned {
            self.watermarks.entry(*partition).or_insert(i64::MIN);
        }
        Ok(assigned)
    }

    fn seek_recovery_offset(
        &mut self,
        after: &OffsetToken,
        assigned: &BTreeSet<i32>,
    ) -> Result<(), SourceError> {
        if after.as_bytes().is_empty() || self.last_polled.as_ref() == Some(after) {
            return Ok(());
        }
        let offsets: BTreeMap<u64, u64> = serde_json::from_slice(after.as_bytes()).map_err(|e| {
            SourceError::PollDeltaFailed {
                reason: format!(
                    "invalid Kafka offset token: {e}. Next steps: recover the token from the committed source epoch"
                ),
            }
        })?;
        if assigned.is_empty() {
            return Ok(());
        }
        let mut positions = TopicPartitionList::new();
        for partition in assigned {
            let offset = offsets.get(&(*partition as u64)).copied().unwrap_or(0);
            let offset = i64::try_from(offset).map_err(|_| SourceError::PollDeltaFailed {
                reason:
                    "Kafka offset exceeds i64. Next steps: restore a valid committed offset token"
                        .to_string(),
            })?;
            positions
                .add_partition_offset(&self.topic, *partition, Offset::Offset(offset))
                .map_err(|error| SourceError::PollDeltaFailed {
                    reason: format!(
                        "Kafka recovery seek setup failed: {error}. Next steps: retry after assignment stabilizes"
                    ),
                })?;
        }
        self.consumer
            .seek_partitions(positions, Duration::from_secs(1))
            .map_err(|error| SourceError::PollDeltaFailed {
                reason: format!(
                    "Kafka recovery seek failed: {error}. Next steps: retry after consumer-group rebalance"
                ),
            })?;
        Ok(())
    }

    fn next_record(&mut self) -> Result<Option<KafkaRecord>, SourceError> {
        if let Some(record) = self.pending_record.take() {
            return Ok(Some(record));
        }
        let receive =
            async { tokio::time::timeout(Duration::from_millis(25), self.consumer.recv()).await };
        let message = match if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(receive))
        } else {
            self.runtime
                .as_ref()
                .expect("runtime exists outside Tokio")
                .block_on(receive)
        } {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "Kafka poll failed: {error}. Next steps: retry after verifying broker connectivity"
                    ),
                });
            }
            Err(_) => return Ok(None),
        };
        let payload = message
            .payload()
            .ok_or_else(|| SourceError::PollDeltaFailed {
                reason: "Kafka record has no payload. Next steps: publish JSON source records"
                    .to_string(),
            })?;
        let offset = u64::try_from(message.offset()).unwrap_or(0);
        let body: KafkaPayload = match serde_json::from_slice(payload) {
            Ok(body) => body,
            Err(error) => {
                rockstream_types::dlq::quarantine_record(
                    &self.topic,
                    offset,
                    "RS-1003",
                    &format!("Kafka record payload is not valid JSON shape: {error}"),
                    payload,
                );
                return self.next_record();
            }
        };
        Ok(Some(KafkaRecord {
            offset,
            partition: message.partition(),
            timestamp: body.timestamp,
            values: body.values,
            weight: body.weight,
            bytes: payload.len(),
        }))
    }

    fn build_batch(&self, records: &[KafkaRecord]) -> Result<Vec<RecordBatch>, SourceError> {
        use rockstream_types::arrow_batch::append_weight_column;

        if records.is_empty() {
            return Ok(vec![]);
        }
        let rows = records
            .iter()
            .map(|record| record.values.clone())
            .collect::<Vec<_>>();
        let weights = records
            .iter()
            .map(|record| record.weight)
            .collect::<Vec<_>>();
        let data = json_rows_to_batch(&self.schema, &rows, "Kafka")?;
        append_weight_column(data, &weights)
            .map(|batch| vec![batch])
            .map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("failed to append Kafka weight column: {error}"),
            })
    }

    /// Last successfully committed epoch/token pair.
    pub fn last_committed(&self) -> Option<(Epoch, OffsetToken)> {
        self.last_committed.clone()
    }
}

#[async_trait]
impl SourceConnector for KafkaSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    async fn capture_snapshot_delta_fence(
        &mut self,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        let assigned = self.refresh_assignment()?;
        let offsets = self
            .last_polled
            .clone()
            .or_else(|| self.last_committed.as_ref().map(|(_, token)| token.clone()))
            .map(Ok)
            .unwrap_or_else(|| {
                serde_json::to_vec(
                    &assigned
                        .iter()
                        .map(|partition| (*partition as u64, 0_u64))
                        .collect::<BTreeMap<_, _>>(),
                )
                .map(OffsetToken::new)
                .map_err(|error| SourceError::Io(format!("Kafka fence encoding failed: {error}")))
            })?;
        Ok(SnapshotDeltaFence::new(offsets.clone(), offsets))
    }

    async fn start_snapshot(
        &mut self,
        _fence: &SnapshotDeltaFence,
        _after: Option<OffsetToken>,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        Ok(SnapshotStream::new(vec![]))
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        self.last_poll_fill_level = usize::from(self.pending_record.is_some());
        if self.paused || credits_available == 0 || max_bytes == 0 {
            return Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: self.current_global_watermark(),
            });
        }
        let assigned = self.refresh_assignment()?;
        self.seek_recovery_offset(&after, &assigned)?;

        let mut offsets: BTreeMap<u64, u64> = if after.as_bytes().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_slice(after.as_bytes()).map_err(|error| SourceError::PollDeltaFailed {
                reason: format!(
                    "invalid Kafka offset token: {error}. Next steps: recover the token from the committed source epoch"
                ),
            })?
        };
        let record_limit = credits_available.min(KAFKA_SOURCE_BUFFER_LIMIT);
        let mut records = Vec::with_capacity(record_limit);
        let mut bytes = 0;
        while records.len() < record_limit {
            let Some(record) = self.next_record()? else {
                break;
            };
            if record.bytes > max_bytes && records.is_empty() {
                self.pending_record = Some(record);
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "Kafka record exceeds max_bytes={max_bytes}. Next steps: increase the bounded poll size"
                    ),
                });
            }
            if bytes + record.bytes > max_bytes {
                self.pending_record = Some(record);
                break;
            }
            bytes += record.bytes;
            offsets.insert(record.partition as u64, record.offset + 1);
            self.watermarks
                .entry(record.partition)
                .and_modify(|watermark| *watermark = (*watermark).max(record.timestamp));
            records.push(record);
        }
        self.last_poll_fill_level = usize::from(self.pending_record.is_some());
        self.refresh_assignment()?;
        let new_offset = OffsetToken::new(serde_json::to_vec(&offsets).map_err(|error| {
            SourceError::PollDeltaFailed {
                reason: format!("failed to serialize Kafka offset token: {error}"),
            }
        })?);
        self.last_polled = Some(new_offset.clone());
        Ok(PollDeltaResult {
            batches: self.build_batch(&records)?,
            new_offset,
            watermark: self.current_global_watermark(),
        })
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        let offsets: BTreeMap<u64, u64> =
            serde_json::from_slice(offset.as_bytes()).map_err(|e| {
                SourceError::CommitOffsetFailed {
                    epoch,
                    reason: format!(
                    "invalid Kafka offset token: {e}. Next steps: commit the emitted source token"
                ),
                }
            })?;
        if offsets.is_empty() {
            self.last_committed = Some((epoch, offset));
            return Ok(());
        }
        let mut commit = TopicPartitionList::new();
        for (partition, next_offset) in offsets {
            let partition =
                i32::try_from(partition).map_err(|_| SourceError::CommitOffsetFailed {
                    epoch,
                    reason: "Kafka partition exceeds i32. Next steps: commit a valid source token"
                        .to_string(),
                })?;
            let next_offset =
                i64::try_from(next_offset).map_err(|_| SourceError::CommitOffsetFailed {
                    epoch,
                    reason: "Kafka offset exceeds i64. Next steps: commit a valid source token"
                        .to_string(),
                })?;
            commit
                .add_partition_offset(&self.topic, partition, Offset::Offset(next_offset))
                .map_err(|error| SourceError::CommitOffsetFailed {
                    epoch,
                    reason: format!(
                        "Kafka commit setup failed: {error}. Next steps: retry after rebalance"
                    ),
                })?;
        }
        self.consumer
            .commit(&commit, CommitMode::Sync)
            .map_err(|error| SourceError::CommitOffsetFailed {
                epoch,
                reason: format!("Kafka commit failed: {error}. Next steps: retry the source epoch"),
            })?;
        self.last_committed = Some((epoch, offset));
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        let assigned = self.consumer.assignment().map_err(|error| {
            SourceError::Io(format!(
                "Kafka assignment lookup failed: {error}. Next steps: retry pause after rebalance"
            ))
        })?;
        self.consumer.pause(&assigned).map_err(|error| {
            SourceError::Io(format!(
                "Kafka pause failed: {error}. Next steps: retry pause after rebalance"
            ))
        })?;
        self.paused = true;
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        let assigned = self.consumer.assignment().map_err(|error| {
            SourceError::Io(format!(
                "Kafka assignment lookup failed: {error}. Next steps: retry resume after rebalance"
            ))
        })?;
        self.consumer.resume(&assigned).map_err(|error| {
            SourceError::Io(format!(
                "Kafka resume failed: {error}. Next steps: retry resume after rebalance"
            ))
        })?;
        self.paused = false;
        Ok(())
    }
}
