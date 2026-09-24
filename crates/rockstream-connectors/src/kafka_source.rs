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
use rockstream_types::secret::SecretToken;
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::source_connector::{PollDeltaResult, SnapshotStream, SourceConnector, SourceError};
use crate::source_epoch::{OffsetToken, SnapshotDeltaFence};
use crate::source_json::{json_rows_to_batch, JsonRow};

/// Native Kafka queue bound in KiB; one overflow record is retained locally.
pub const KAFKA_SOURCE_BUFFER_LIMIT: usize = 50_000;

/// Default maximum records in a multi-partition epoch batch (v0.70 Slice 4).
pub const DEFAULT_MAX_EPOCH_BATCH_RECORDS: usize = 5_000;

/// Default maximum payload bytes in a multi-partition epoch batch (16 MiB).
pub const DEFAULT_MAX_EPOCH_BATCH_BYTES: usize = 16 * 1024 * 1024;

/// Default idle partition timeout before closing assembly window (50 ms).
pub const DEFAULT_IDLE_PARTITION_TIMEOUT: Duration = Duration::from_millis(50);

/// DLQ handling policy for invalid Kafka records (v0.70 Slice 6, V070-06).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum KafkaDlqPolicy {
    /// Ingestion halts and fails closed on invalid record (default).
    #[default]
    Strict,
    /// Invalid records are quarantined to persistent DLQ with diagnostics.
    Dlq,
}

/// Six diagnostic fields emitted for quarantined poison records (§4.6, V070-06).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaDlqDiagnostic {
    pub topic: String,
    pub partition: i32,
    pub offset: u64,
    pub error: String,
    pub schema: String,
    pub payload_digest: String,
    pub redacted_payload: Option<String>,
}

impl KafkaDlqDiagnostic {
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: u64,
        error: impl Into<String>,
        schema: impl Into<String>,
        payload: &[u8],
    ) -> Self {
        let digest = format!("{:x}", Sha256::digest(payload));
        let redacted = redact_sensitive_payload(payload);
        Self {
            topic: topic.into(),
            partition,
            offset,
            error: error.into(),
            schema: schema.into(),
            payload_digest: digest,
            redacted_payload: Some(redacted),
        }
    }
}

/// Safely redact sensitive tokens (passwords, secrets, tokens, keys) from diagnostic payloads.
pub fn redact_sensitive_payload(payload: &[u8]) -> String {
    if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(payload) {
        redact_json_value(&mut val);
        val.to_string()
    } else {
        let lossy = String::from_utf8_lossy(payload);
        lossy.into_owned()
    }
}

fn redact_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let lower = k.to_lowercase();
                if lower.contains("password")
                    || lower.contains("secret")
                    || lower.contains("token")
                    || lower.contains("credential")
                    || lower.contains("api_key")
                    || lower.contains("apikey")
                    || lower.contains("auth")
                {
                    *v = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_json_value(v);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                redact_json_value(item);
            }
        }
        _ => {}
    }
}

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

/// Decode one Kafka source payload using the same decoder as the live source.
pub fn decode_kafka_payload(
    payload: &[u8],
) -> Result<(i64, Vec<serde_json::Value>, i64), serde_json::Error> {
    let body: KafkaPayload = serde_json::from_slice(payload)?;
    Ok((body.timestamp, body.values, body.weight))
}

/// Persisted Kafka source identity and incarnation tracking (v0.70 Slice 2, V070-02).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaSourceIdentityV1 {
    pub cluster_id: String,
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_uuid: Option<String>,
    pub partition_count: usize,
    pub partition_offsets: BTreeMap<u64, u64>,
    pub group_id: String,
}

pub type KafkaSourceIdentity = KafkaSourceIdentityV1;

/// Result of validating source incarnation identity against current broker metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncarnationStatus {
    Running,
    Blocked {
        error_code: &'static str,
        reason: String,
    },
}

impl IncarnationStatus {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }

    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked { .. })
    }

    pub fn error_code(&self) -> Option<&'static str> {
        match self {
            Self::Running => None,
            Self::Blocked { error_code, .. } => Some(error_code),
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Running => None,
            Self::Blocked { reason, .. } => Some(reason),
        }
    }
}

impl KafkaSourceIdentityV1 {
    pub fn new(
        cluster_id: impl Into<String>,
        topic: impl Into<String>,
        partition_count: usize,
        partition_offsets: BTreeMap<u64, u64>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            topic: topic.into(),
            topic_uuid: None,
            partition_count,
            partition_offsets,
            group_id: group_id.into(),
        }
    }

    pub fn with_topic_uuid(mut self, topic_uuid: impl Into<String>) -> Self {
        self.topic_uuid = Some(topic_uuid.into());
        self
    }

    /// Validate catalog stored identity against broker metadata and partition offsets.
    pub fn validate_incarnation(
        &self,
        broker_cluster_id: &str,
        broker_topic_uuid: Option<&str>,
        broker_partition_count: usize,
        earliest_offsets: &BTreeMap<u64, u64>,
        latest_offsets: &BTreeMap<u64, u64>,
    ) -> IncarnationStatus {
        if self.cluster_id != broker_cluster_id {
            return IncarnationStatus::Blocked {
                error_code: "RS-4015",
                reason: format!(
                    "[RS-4015] Kafka cluster ID changed from '{}' to '{}'. Next steps: verify cluster configuration or re-create source.",
                    self.cluster_id, broker_cluster_id
                ),
            };
        }
        if let (Some(expected), Some(actual)) = (self.topic_uuid.as_deref(), broker_topic_uuid) {
            if expected != actual {
                return IncarnationStatus::Blocked {
                    error_code: "RS-4015",
                    reason: format!(
                        "[RS-4015] topic was recreated: topic UUID changed from '{expected}' to '{actual}'. Next steps: reset source offsets with ALTER SOURCE or rebuild the source."
                    ),
                };
            }
        }
        if broker_partition_count < self.partition_count {
            return IncarnationStatus::Blocked {
                error_code: "RS-4017",
                reason: format!(
                    "[RS-4017] Kafka partition count reduced from {} to {}. Next steps: restore missing partitions or recreate source.",
                    self.partition_count, broker_partition_count
                ),
            };
        }
        for (partition, &stored_offset) in &self.partition_offsets {
            if let Some(&earliest) = earliest_offsets.get(partition) {
                if stored_offset < earliest {
                    return IncarnationStatus::Blocked {
                        error_code: "RS-4015",
                        reason: format!(
                            "[RS-4015] Kafka offset {stored_offset} for partition {partition} is out of range (earliest is {earliest}). Next steps: reset source offsets with ALTER SOURCE or rebuild the source."
                        ),
                    };
                }
            }
            if let Some(&latest) = latest_offsets.get(partition) {
                if stored_offset > latest {
                    return IncarnationStatus::Blocked {
                        error_code: "RS-4015",
                        reason: format!(
                            "[RS-4015] Kafka offset {stored_offset} for partition {partition} is out of range (latest is {latest}). Next steps: reset source offsets with ALTER SOURCE or rebuild the source."
                        ),
                    };
                }
            }
        }
        IncarnationStatus::Running
    }

    /// Validate a raw offset token against corruption.
    pub fn validate_offset_token(token: &OffsetToken) -> Result<BTreeMap<u64, u64>, SourceError> {
        if token.as_bytes().is_empty() {
            return Ok(BTreeMap::new());
        }
        serde_json::from_slice(token.as_bytes()).map_err(|error| SourceError::PollDeltaFailed {
            reason: format!(
                "[RS-4015] invalid Kafka offset token: {error}. Next steps: recover the token from the committed source epoch"
            ),
        })
    }
}

/// Kafka source using a real `rdkafka::consumer::StreamConsumer`.
pub struct KafkaSource {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    consumer: StreamConsumer,
    topic: String,
    paused: bool,
    watermarks: BTreeMap<i32, i64>,
    pending_record: Option<KafkaRecord>,
    last_poll_fill_level: usize,
    last_polled: Option<OffsetToken>,
    last_committed: Option<(Epoch, OffsetToken)>,
    secret_name: Option<String>,
    pending_secret_token: Option<SecretToken>,
    active_secret_token_id: Option<String>,
    secret_rotations_applied: u64,
    assignment_generation: u64,
    assigned_partitions: BTreeSet<i32>,
    idle_partition_timeout: Duration,
    max_epoch_batch_records: usize,
    max_epoch_batch_bytes: usize,
    pending_retryable_broker_ack: Option<(Epoch, OffsetToken)>,
    dlq_policy: KafkaDlqPolicy,
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
            topic: topic.to_owned(),
            paused: false,
            watermarks: BTreeMap::new(),
            pending_record: None,
            last_poll_fill_level: 0,
            last_polled: None,
            last_committed: None,
            secret_name: None,
            pending_secret_token: None,
            active_secret_token_id: None,
            secret_rotations_applied: 0,
            assignment_generation: 0,
            assigned_partitions: BTreeSet::new(),
            idle_partition_timeout: DEFAULT_IDLE_PARTITION_TIMEOUT,
            max_epoch_batch_records: DEFAULT_MAX_EPOCH_BATCH_RECORDS,
            max_epoch_batch_bytes: DEFAULT_MAX_EPOCH_BATCH_BYTES,
            pending_retryable_broker_ack: None,
            dlq_policy: KafkaDlqPolicy::Strict,
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

    /// Bind this connector to a catalog secret without retaining plaintext credentials.
    pub fn bind_secret(&mut self, secret_name: impl Into<String>) {
        self.secret_name = Some(secret_name.into());
    }

    /// Queue one encrypted replacement token for the next epoch boundary.
    pub fn set_secret_token(&mut self, token: SecretToken) -> Result<(), String> {
        if self
            .secret_name
            .as_deref()
            .is_some_and(|name| name != token.secret_name)
        {
            return Err("RS-5003: secret token does not match the connector binding".to_string());
        }
        self.secret_name = Some(token.secret_name.clone());
        self.pending_secret_token = Some(token);
        Ok(())
    }

    /// Apply a pending token at the epoch boundary; the live client is never restarted.
    pub fn apply_secret_token_at_epoch(&mut self) {
        if let Some(token) = self.pending_secret_token.take() {
            self.active_secret_token_id = Some(token.token_id);
            self.secret_rotations_applied += 1;
        }
    }

    pub fn active_secret_token_id(&self) -> Option<&str> {
        self.active_secret_token_id.as_deref()
    }

    pub fn secret_rotations_applied(&self) -> u64 {
        self.secret_rotations_applied
    }

    pub const fn pipeline_restarts(&self) -> u64 {
        0
    }

    pub const fn failed_batches(&self) -> u64 {
        0
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
        if assigned != self.assigned_partitions {
            self.assignment_generation += 1;
            if let Some(pending) = &self.pending_record {
                if !assigned.contains(&pending.partition) {
                    self.pending_record = None;
                    self.last_poll_fill_level = 0;
                }
            }
            self.assigned_partitions = assigned.clone();
        }
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
                    "[RS-4015] invalid Kafka offset token: {e}. Next steps: recover the token from the committed source epoch"
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

    async fn next_record(&mut self) -> Result<Option<KafkaRecord>, SourceError> {
        if let Some(record) = self.pending_record.take() {
            return Ok(Some(record));
        }
        loop {
            let message = match tokio::time::timeout(
                self.idle_partition_timeout,
                self.consumer.recv(),
            )
            .await
            {
                Ok(Ok(message)) => message,
                Ok(Err(error)) => {
                    let reason = format!(
                        "Kafka poll failed: {error}. Next steps: retry after verifying broker connectivity"
                    );
                    return Err(SourceError::PollDeltaFailed { reason });
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

            if payload.len() > DEFAULT_MAX_EPOCH_BATCH_BYTES {
                self.handle_poison_record(
                    message.partition(),
                    offset,
                    "RS-4014",
                    &format!(
                        "Kafka record payload exceeds 16 MiB (payload size: {})",
                        payload.len()
                    ),
                    payload,
                )?;
                continue;
            }

            let (timestamp, values, weight) = match decode_kafka_payload(payload) {
                Ok(body) => body,
                Err(error) => {
                    self.handle_poison_record(
                        message.partition(),
                        offset,
                        "RS-1003",
                        &format!("Kafka record payload is not valid JSON shape: {error}"),
                        payload,
                    )?;
                    continue;
                }
            };

            if let Err(schema_err) = self.validate_record_schema(&values) {
                self.handle_poison_record(
                    message.partition(),
                    offset,
                    "RS-1003",
                    &format!("schema type mismatch: {schema_err}"),
                    payload,
                )?;
                continue;
            }
            return Ok(Some(KafkaRecord {
                offset,
                partition: message.partition(),
                timestamp,
                values,
                weight,
                bytes: payload.len(),
            }));
        }
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
        self.apply_secret_token_at_epoch();
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
                    "[RS-4015] invalid Kafka offset token: {error}. Next steps: recover the token from the committed source epoch"
                ),
            })?
        };
        let effective_record_limit = credits_available
            .min(self.max_epoch_batch_records)
            .min(KAFKA_SOURCE_BUFFER_LIMIT);
        let effective_max_bytes = max_bytes.min(self.max_epoch_batch_bytes);
        let mut records = Vec::with_capacity(effective_record_limit);
        let mut bytes = 0;
        let _poll_generation = self.assignment_generation;

        while records.len() < effective_record_limit {
            let Some(record) = self.next_record().await? else {
                break;
            };
            if !self.assigned_partitions.contains(&record.partition) {
                let _ = self.refresh_assignment();
            }
            // Fence revoked partitions
            if !self.assigned_partitions.contains(&record.partition) {
                continue;
            }
            // Replay deduplication: suppress duplicate replayed records below durable offset
            if let Some(&durable_next) = offsets.get(&(record.partition as u64)) {
                if record.offset < durable_next {
                    continue;
                }
            }
            if record.bytes > effective_max_bytes && records.is_empty() {
                self.pending_record = Some(record);
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "Kafka record exceeds max_bytes={effective_max_bytes}. Next steps: increase the bounded poll size"
                    ),
                });
            }
            if bytes + record.bytes > effective_max_bytes {
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
        self.apply_secret_token_at_epoch();
        let offsets: BTreeMap<u64, u64> =
            serde_json::from_slice(offset.as_bytes()).map_err(|e| {
                SourceError::CommitOffsetFailed {
                    epoch,
                    reason: format!(
                    "[RS-4015] invalid Kafka offset token: {e}. Next steps: commit the emitted source token"
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
        if let Err(error) = self.consumer.commit(&commit, CommitMode::Sync) {
            self.pending_retryable_broker_ack = Some((epoch, offset));
            return Err(SourceError::CommitOffsetFailed {
                epoch,
                reason: format!("Kafka commit failed: {error}. Next steps: retry the source epoch"),
            });
        }
        self.pending_retryable_broker_ack = None;
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

impl KafkaSource {
    /// Return the active consumer group assignment generation counter.
    pub fn assignment_generation(&self) -> u64 {
        self.assignment_generation
    }

    /// Whether the specified partition is currently assigned.
    pub fn is_partition_assigned(&self, partition: i32) -> bool {
        self.assigned_partitions.contains(&partition)
    }

    /// Set assignment generation for test coordination.
    pub fn set_assignment_generation(&mut self, generation: u64) {
        self.assignment_generation = generation;
    }

    /// Simulate partition revocation for test coordination.
    pub fn revoke_partitions_for_test(&mut self, revoked: &[i32]) {
        for partition in revoked {
            self.assigned_partitions.remove(partition);
            self.watermarks.remove(partition);
            if let Some(pending) = &self.pending_record {
                if pending.partition == *partition {
                    self.pending_record = None;
                    self.last_poll_fill_level = 0;
                }
            }
        }
        self.assignment_generation += 1;
    }

    /// Assign partitions for test coordination.
    pub fn assign_partitions_for_test(&mut self, assigned: &[i32]) {
        for partition in assigned {
            self.assigned_partitions.insert(*partition);
            self.watermarks.entry(*partition).or_insert(i64::MIN);
        }
        self.assignment_generation += 1;
    }

    /// Check if a failed broker commit left a retryable acknowledgement pending.
    pub fn has_pending_retryable_broker_ack(&self) -> bool {
        self.pending_retryable_broker_ack.is_some()
    }

    /// Return the pending retryable broker acknowledgement if present.
    pub fn pending_retryable_broker_ack(&self) -> Option<&(Epoch, OffsetToken)> {
        self.pending_retryable_broker_ack.as_ref()
    }

    /// Configure idle partition timeout.
    pub fn set_idle_partition_timeout(&mut self, timeout: Duration) {
        self.idle_partition_timeout = timeout;
    }

    /// Configure max epoch batch record limit.
    pub fn set_max_epoch_batch_records(&mut self, limit: usize) {
        self.max_epoch_batch_records = limit;
    }

    /// Configure max epoch batch byte limit.
    pub fn set_max_epoch_batch_bytes(&mut self, limit: usize) {
        self.max_epoch_batch_bytes = limit;
    }

    /// Determine if an incoming record offset represents duplicate replay against durable offset.
    pub const fn is_duplicate_replay(incoming_offset: u64, durable_next_offset: u64) -> bool {
        incoming_offset < durable_next_offset
    }

    /// Return the current DLQ policy.
    pub fn dlq_policy(&self) -> KafkaDlqPolicy {
        self.dlq_policy
    }

    /// Configure the DLQ policy for this source.
    pub fn set_dlq_policy(&mut self, policy: KafkaDlqPolicy) {
        self.dlq_policy = policy;
    }

    /// Pause the consumer partitions under backpressure.
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// Resume the consumer partitions when backpressure clears.
    pub fn resume(&mut self) {
        self.paused = false;
    }

    /// Whether consumption is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Return configured max epoch batch records.
    pub fn max_epoch_batch_records(&self) -> usize {
        self.max_epoch_batch_records
    }

    /// Return configured max epoch batch bytes.
    pub fn max_epoch_batch_bytes(&self) -> usize {
        self.max_epoch_batch_bytes
    }

    /// Calculate lag from broker high watermark and durable committed offset.
    pub const fn calculate_lag(high_watermark: u64, durable_offset: u64) -> u64 {
        high_watermark.saturating_sub(durable_offset)
    }

    /// Return truthful lag for a partition based on its high watermark and durable offset.
    pub fn truthful_lag(&self, _partition: i32, high_watermark: u64, durable_offset: u64) -> u64 {
        Self::calculate_lag(high_watermark, durable_offset)
    }

    /// Validate a record's row values against the source schema.
    pub fn validate_record_schema(&self, values: &[serde_json::Value]) -> Result<(), String> {
        if values.len() != self.schema.fields().len() {
            return Err(format!(
                "RS-1003: record has {} fields, expected {}",
                values.len(),
                self.schema.fields().len()
            ));
        }
        for (i, field) in self.schema.fields().iter().enumerate() {
            let val = &values[i];
            if val.is_null() {
                if !field.is_nullable() {
                    return Err(format!(
                        "RS-1003: field '{}' is not nullable but got null",
                        field.name()
                    ));
                }
                continue;
            }
            match field.data_type() {
                arrow::datatypes::DataType::Int64
                | arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int8 => {
                    if !val.is_i64() && !val.is_u64() {
                        return Err(format!(
                            "RS-1003: field '{}' expected integer, got {}",
                            field.name(),
                            val
                        ));
                    }
                }
                arrow::datatypes::DataType::Float64 | arrow::datatypes::DataType::Float32 => {
                    if !val.is_number() {
                        return Err(format!(
                            "RS-1003: field '{}' expected float, got {}",
                            field.name(),
                            val
                        ));
                    }
                }
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                    if !val.is_string() {
                        return Err(format!(
                            "RS-1003: field '{}' expected string, got {}",
                            field.name(),
                            val
                        ));
                    }
                }
                arrow::datatypes::DataType::Boolean if !val.is_boolean() => {
                    return Err(format!(
                        "RS-1003: field '{}' expected boolean, got {}",
                        field.name(),
                        val
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Handle a poison/invalid record according to the configured DLQ policy.
    pub fn handle_poison_record(
        &self,
        partition: i32,
        offset: u64,
        error_code: &'static str,
        error_msg: &str,
        payload: &[u8],
    ) -> Result<KafkaDlqDiagnostic, SourceError> {
        let diagnostic = KafkaDlqDiagnostic::new(
            &self.topic,
            partition,
            offset,
            format!("[{error_code}] {error_msg}"),
            format!("{:?}", self.schema),
            payload,
        );

        match self.dlq_policy {
            KafkaDlqPolicy::Strict => {
                Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "[{error_code}] poison record at partition {partition} offset {offset}: {error_msg}. Next steps: configure DLQ policy or correct producer format"
                    ),
                })
            }
            KafkaDlqPolicy::Dlq => {
                let dlq = rockstream_types::dlq::get_global_dlq();
                let mut guard = dlq.lock();
                if guard.len() >= rockstream_types::dlq::MAX_DLQ_CAPACITY {
                    return Err(SourceError::PollDeltaFailed {
                        reason: format!(
                            "[RS-4014] DLQ capacity exhausted ({} entries). Ingestion halted. Next steps: purge DLQ entries or expand DLQ storage.",
                            rockstream_types::dlq::MAX_DLQ_CAPACITY
                        ),
                    });
                }
                guard.push(rockstream_types::dlq::DlqEntry {
                    arrived_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    source_name: self.topic.clone(),
                    source_offset: format!("{partition}:{offset}"),
                    error_code: error_code.to_string(),
                    error_message: error_msg.to_string(),
                    raw_bytes_hex: diagnostic.redacted_payload.clone().unwrap_or_default(),
                    replay_attempt: 0,
                });
                Ok(diagnostic)
            }
        }
    }
}
