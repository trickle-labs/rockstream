//! Bounded PostgreSQL logical-replication source support.
//!
//! The replication worker owns the network socket; this type owns only the
//! decoded, credit-controlled handoff into the source epoch.  Consequently a
//! zero-credit poll never consumes another replication message.

#![deny(clippy::unwrap_used, clippy::expect_used)]

use async_trait::async_trait;
use std::collections::VecDeque;

use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_types::arrow_batch::append_weight_column;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::secret::SecretToken;
use rockstream_types::timestamp::Epoch;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls};

use crate::source_connector::{
    PollDeltaResult, SnapshotBatch, SnapshotStream, SourceConnector, SourceError,
    WatermarkCapability,
};
use crate::source_epoch::{OffsetToken, SnapshotDeltaFence};

/// Maximum decoded records retained before replication reads are paused.
pub const POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS: usize = 4_096;
/// Maximum decoded payload bytes retained before replication reads are paused.
pub const POSTGRES_CDC_MAX_IN_FLIGHT_BYTES: usize = 8 * 1024 * 1024;
/// Maximum bytes retained for one decoded upstream transaction.
pub const POSTGRES_CDC_MAX_TRANSACTION_BYTES: usize = POSTGRES_CDC_MAX_IN_FLIGHT_BYTES;
/// Hot-memory share of one transaction before the gateway coordinator spills.
pub const POSTGRES_CDC_TRANSACTION_MEMORY_BYTES: usize = POSTGRES_CDC_MAX_TRANSACTION_BYTES / 2;
/// WAL lag at which the source stops receiving more replication data.
pub const POSTGRES_CDC_MAX_WAL_LAG_BYTES: u64 = 256 * 1024 * 1024;
/// Resnapshot attempts are bounded so a broken publication cannot loop forever.
pub const POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS: u8 = 3;

/// PostgreSQL LSN encoded as the native 64-bit WAL location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgLsn(pub u64);

impl PgLsn {
    pub const ZERO: Self = Self(0);

    pub fn parse(input: &str) -> Result<Self, SourceError> {
        let (high, low) = input
            .split_once('/')
            .ok_or_else(|| SourceError::PollDeltaFailed {
                reason: format!("invalid PostgreSQL LSN '{input}'; expected HEX/HEX"),
            })?;
        let high = u32::from_str_radix(high, 16).map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("invalid PostgreSQL LSN '{input}'; expected HEX/HEX"),
        })?;
        let low = u32::from_str_radix(low, 16).map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("invalid PostgreSQL LSN '{input}'; expected HEX/HEX"),
        })?;
        Ok(Self(((high as u64) << 32) | low as u64))
    }

    pub fn to_offset_token(self) -> OffsetToken {
        OffsetToken::new(self.0.to_be_bytes().to_vec())
    }

    pub fn from_offset_token(token: &OffsetToken) -> Result<Self, SourceError> {
        if token.as_bytes().is_empty() {
            return Ok(Self::ZERO);
        }
        let bytes: [u8; 8] =
            token
                .as_bytes()
                .try_into()
                .map_err(|_| SourceError::PollDeltaFailed {
                    reason: "PostgreSQL CDC offset must contain one packed 64-bit LSN".to_string(),
                })?;
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

impl std::fmt::Display for PgLsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcWireFormat {
    PgOutput,
    Wal2Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresCdcFailure {
    SlotInvalidated,
    PublicationMissing,
    ReplicationTimeout,
    WalRetentionLost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresCdcStatus {
    Running,
    Blocked { code: &'static str, reason: String },
    Resnapshotting { attempt: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcChange {
    pub lsn: PgLsn,
    pub table_id: u32,
    pub primary_key: Vec<u8>,
    pub row_id: u64,
    pub operation: CdcOperation,
    pub old_values: Option<Vec<i64>>,
    pub new_values: Option<Vec<i64>>,
}

/// A pgoutput transaction is the smallest unit made visible to a source epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcTransactionEnvelope {
    pub xid: u32,
    pub end_lsn: PgLsn,
    pub changes: Vec<CdcChange>,
}

/// One pgoutput relation column as described on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgOutputColumn {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// Relation metadata retained by the source-scoped gateway router.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgOutputRelationMetadata {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<PgOutputColumn>,
}

/// Relation-aware protocol events. Row events always carry their enclosing
/// xid; acknowledging an offset is intentionally not part of this API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgOutputEvent {
    Begin {
        xid: u32,
    },
    Relation {
        xid: u32,
        relation: PgOutputRelationMetadata,
    },
    Insert {
        xid: u32,
        relation_id: u32,
        new_values: Vec<Option<String>>,
    },
    Update {
        xid: u32,
        relation_id: u32,
        old_values: Vec<Option<String>>,
        new_values: Vec<Option<String>>,
    },
    Delete {
        xid: u32,
        relation_id: u32,
        old_values: Vec<Option<String>>,
    },
    Commit {
        xid: u32,
        commit_lsn: PgLsn,
    },
}

#[derive(Debug, Clone)]
pub struct PgOutputSnapshotRelation {
    pub relation: PgOutputRelationMetadata,
    pub column_policies: Vec<(bool, bool)>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Clone)]
pub struct PgOutputSourceSnapshot {
    pub lsn: PgLsn,
    pub relations: Vec<PgOutputSnapshotRelation>,
}

impl CdcChange {
    /// Stable identity deliberately excludes LSN: updates retract/reinsert the
    /// same key while the LSN remains version metadata.
    pub fn row_id_for(table_id: u32, primary_key: &[u8]) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in table_id.to_be_bytes().iter().chain(primary_key.iter()) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

#[derive(Deserialize)]
struct Wal2JsonChange {
    lsn: String,
    table_id: u32,
    op: String,
    key: String,
    old: Option<Vec<i64>>,
    new: Option<Vec<i64>>,
}

/// A decoded CDC source with named bounded handoff capacity and fill metrics.
pub struct PostgresCdcSource {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    format: CdcWireFormat,
    queued: VecDeque<QueuedChange>,
    queued_bytes: usize,
    snapshot_batches: Vec<RecordBatch>,
    manually_paused: bool,
    replication_read_paused: bool,
    wal_lag_bytes: u64,
    recovery_attempts: u8,
    status: PostgresCdcStatus,
    committed: Option<(Epoch, PgLsn)>,
    pgoutput: Option<PgOutputConnection>,
    pgoutput_config: Option<PgOutputConfig>,
    transaction: Option<(u32, usize)>,
    last_decoded_envelope: Option<CdcTransactionEnvelope>,
    transaction_changes: Vec<CdcChange>,
    native_transaction: Option<(u32, Vec<PgOutputTextChange>, usize)>,
    native_seen_messages: usize,
    native_xid: Option<u32>,
    secret_name: Option<String>,
    pending_secret_token: Option<SecretToken>,
    active_secret_token_id: Option<String>,
    secret_rotations_applied: u64,
}

/// Connection details for a native pgoutput source. Credentials are resolved
/// by the gateway before this value is constructed.
#[derive(Clone)]
pub struct PgOutputConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    pub slot: String,
    pub publication: String,
    pub table: String,
}

struct PgOutputConnection {
    client: Client,
    slot: String,
    publication: String,
    table: String,
    snapshot_lsn: Option<PgLsn>,
    relations: std::collections::HashMap<u32, PgOutputRelation>,
}

#[derive(Debug, Clone)]
struct PgOutputRelation {
    columns: usize,
}

#[derive(Debug, Clone)]
struct PgOutputTextChange {
    operation: CdcOperation,
    old_values: Option<Vec<String>>,
    new_values: Option<Vec<String>>,
}

struct QueuedChange {
    change: CdcChange,
    bytes: usize,
    transaction_end: bool,
}

impl PostgresCdcSource {
    pub fn new(connector_id: ConnectorId, schema: SchemaRef, format: CdcWireFormat) -> Self {
        Self {
            _connector_id: connector_id,
            schema,
            format,
            queued: VecDeque::new(),
            queued_bytes: 0,
            snapshot_batches: Vec::new(),
            manually_paused: false,
            replication_read_paused: false,
            wal_lag_bytes: 0,
            recovery_attempts: 0,
            status: PostgresCdcStatus::Running,
            committed: None,
            pgoutput: None,
            pgoutput_config: None,
            transaction: None,
            last_decoded_envelope: None,
            transaction_changes: Vec::new(),
            native_transaction: None,
            native_seen_messages: 0,
            native_xid: None,
            secret_name: None,
            pending_secret_token: None,
            active_secret_token_id: None,
            secret_rotations_applied: 0,
        }
    }

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

    /// Connect to PostgreSQL's native pgoutput logical-decoding stream.
    ///
    /// This intentionally supports only PostgreSQL's text tuple representation.
    /// Refusing another physical schema is safer than
    /// coercing a change before it reaches the M3 fence.
    pub async fn connect_pgoutput(
        connector_id: ConnectorId,
        schema: SchemaRef,
        config: PgOutputConfig,
    ) -> Result<Self, SourceError> {
        let mut source = Self::configured_pgoutput(connector_id, schema, config)?;
        source.open_pgoutput(false).await?;
        Ok(source)
    }

    /// Build the transport without opening PostgreSQL. The gateway uses this
    /// so recovery and owner fencing finish before a physical connection is
    /// created.
    pub fn configured_pgoutput(
        connector_id: ConnectorId,
        schema: SchemaRef,
        config: PgOutputConfig,
    ) -> Result<Self, SourceError> {
        if schema.fields().iter().any(|field| {
            !matches!(
                field.data_type(),
                DataType::Int64 | DataType::Int32 | DataType::Utf8 | DataType::Decimal128(_, _)
            )
        }) {
            return Err(SourceError::DiscoverSchemaFailed {
                reason: "native pgoutput currently requires INT, BIGINT, TEXT, or DECIMAL bound source columns".to_string(),
            });
        }
        quote_relation(&config.table)?;
        let mut source = Self::new(connector_id, schema, CdcWireFormat::PgOutput);
        source.pgoutput_config = Some(config);
        Ok(source)
    }

    /// Open and validate the configured slot after recovery and owner lease
    /// acquisition. A checkpointed source may never silently create a slot.
    pub async fn open_pgoutput(&mut self, has_checkpoint: bool) -> Result<(), SourceError> {
        if self.pgoutput.is_some() {
            return Ok(());
        }
        let config = self.pgoutput_config.clone().ok_or_else(|| {
            SourceError::Io("native pgoutput configuration is unavailable".to_string())
        })?;
        let mut connection_config = tokio_postgres::Config::new();
        connection_config
            .host(&config.host)
            .port(config.port)
            .dbname(&config.database)
            .user(&config.user);
        if let Some(password) = &config.password {
            connection_config.password(password);
        }
        let (client, connection) = connection_config
            .connect(NoTls)
            .await
            .map_err(|error| SourceError::Io(format!("connect PostgreSQL CDC source: {error}")))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let slot = client
            .query_opt(
                "SELECT plugin, database, active FROM pg_replication_slots WHERE slot_name = $1",
                &[&config.slot],
            )
            .await
            .map_err(|error| SourceError::Io(format!("inspect pgoutput slot: {error}")))?;
        match slot {
            Some(row) => {
                let plugin = row.get::<_, String>(0);
                let database = row.get::<_, String>(1);
                let active = row.get::<_, bool>(2);
                if plugin != "pgoutput" || database != config.database || active {
                    return Err(SourceError::Io(format!(
                        "RS-4013: pgoutput slot '{}' is incompatible or active elsewhere; next_steps: stop the existing owner and verify plugin/database",
                        config.slot
                    )));
                }
            }
            None if has_checkpoint => {
                return Err(SourceError::Io(format!(
                    "RS-4011: checkpointed pgoutput slot '{}' is missing; next_steps: run the bounded resnapshot workflow",
                    config.slot
                )));
            }
            None => {}
        }
        self.pgoutput = Some(PgOutputConnection {
            client,
            slot: config.slot,
            publication: config.publication,
            table: quote_relation(&config.table)?,
            snapshot_lsn: None,
            relations: std::collections::HashMap::new(),
        });
        Ok(())
    }

    pub fn set_snapshot_batches(&mut self, batches: Vec<RecordBatch>) {
        self.snapshot_batches = batches;
    }

    pub fn close_pgoutput(&mut self) {
        self.pgoutput = None;
        self.native_xid = None;
        self.native_seen_messages = 0;
    }

    pub async fn confirmed_flush_lsn(&self) -> Result<Option<PgLsn>, SourceError> {
        let pgoutput = self.pgoutput.as_ref().ok_or_else(|| {
            SourceError::Io("native pgoutput connection is unavailable".to_string())
        })?;
        let row = pgoutput
            .client
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&pgoutput.slot],
            )
            .await
            .map_err(|error| SourceError::Io(format!("inspect pgoutput checkpoint: {error}")))?;
        row.and_then(|row| row.get::<_, Option<String>>(0))
            .map(|lsn| PgLsn::parse(&lsn))
            .transpose()
    }

    /// Capture all imported relations through one repeatable-read transaction
    /// and one source-wide WAL fence.
    pub async fn capture_source_snapshot(
        &mut self,
        relations: &[(String, SchemaRef)],
    ) -> Result<PgOutputSourceSnapshot, SourceError> {
        let pgoutput = self.pgoutput.as_mut().ok_or_else(|| {
            SourceError::Io("native pgoutput connection is unavailable".to_string())
        })?;
        if pgoutput
            .client
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&pgoutput.slot],
            )
            .await
            .map_err(|error| SourceError::StartSnapshotFailed {
                reason: format!("inspect pgoutput slot: {error}"),
            })?
            .is_none()
        {
            pgoutput
                .client
                .query_one(
                    "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&pgoutput.slot],
                )
                .await
                .map_err(|error| SourceError::StartSnapshotFailed {
                    reason: format!("create pgoutput slot '{}': {error}", pgoutput.slot),
                })?;
        }
        pgoutput
            .client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|error| SourceError::StartSnapshotFailed {
                reason: format!("begin source-wide PostgreSQL snapshot: {error}"),
            })?;
        let result = async {
            let snapshot_lsn: String = pgoutput
                .client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await
                .map_err(|error| SourceError::StartSnapshotFailed {
                    reason: format!("capture source-wide PostgreSQL LSN: {error}"),
                })?
                .get(0);
            let mut snapshots = Vec::with_capacity(relations.len());
            for (table, schema) in relations {
                let metadata = pgoutput
                    .client
                    .query(
                        "SELECT c.oid::bigint, n.nspname, c.relname, c.relreplident::text, a.attname, a.atttypid::bigint, a.atttypmod, CASE WHEN c.relreplident = 'f' OR EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary AND a.attnum = ANY(i.indkey::smallint[])) THEN 1 ELSE 0 END, NOT a.attnotnull, a.atthasdef FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute a ON a.attrelid = c.oid WHERE c.oid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                        &[table],
                    )
                    .await
                    .map_err(|error| SourceError::StartSnapshotFailed {
                        reason: format!("inspect imported relation '{table}': {error}"),
                    })?;
                let first = metadata.first().ok_or_else(|| SourceError::StartSnapshotFailed {
                    reason: format!("imported relation '{table}' has no columns"),
                })?;
                let relation_id = u32::try_from(first.get::<_, i64>(0)).map_err(|_| {
                    SourceError::StartSnapshotFailed {
                        reason: format!("relation OID for '{table}' is out of range"),
                    }
                })?;
                let relation = PgOutputRelationMetadata {
                    relation_id,
                    namespace: first.get(1),
                    name: first.get(2),
                    replica_identity: first
                        .get::<_, String>(3)
                        .bytes()
                        .next()
                        .ok_or_else(|| SourceError::StartSnapshotFailed {
                            reason: format!("relation '{table}' has no replica identity"),
                        })?,
                    columns: metadata
                        .iter()
                        .map(|row| {
                            Ok(PgOutputColumn {
                                flags: u8::try_from(row.get::<_, i32>(7)).map_err(|_| {
                                    SourceError::StartSnapshotFailed {
                                        reason: format!("invalid key flag for '{table}'"),
                                    }
                                })?,
                                name: row.get(4),
                                type_oid: u32::try_from(row.get::<_, i64>(5)).map_err(|_| {
                                    SourceError::StartSnapshotFailed {
                                        reason: format!("column OID for '{table}' is out of range"),
                                    }
                                })?,
                                type_modifier: row.get(6),
                            })
                        })
                        .collect::<Result<Vec<_>, SourceError>>()?,
                };
                if relation.columns.len() != schema.fields().len() {
                    return Err(SourceError::StartSnapshotFailed {
                        reason: format!(
                            "relation '{table}' has {} columns but its imported schema has {}",
                            relation.columns.len(),
                            schema.fields().len()
                        ),
                    });
                }
                let projection = schema
                    .fields()
                    .iter()
                    .map(|field| format!("\"{}\"::text", field.name().replace('"', "\"\"")))
                    .collect::<Vec<_>>()
                    .join(", ");
                let table = quote_relation(table)?;
                let rows = pgoutput
                    .client
                    .query(&format!("SELECT {projection} FROM {table}"), &[])
                    .await
                    .map_err(|error| SourceError::StartSnapshotFailed {
                        reason: format!("read imported relation '{table}': {error}"),
                    })?
                    .into_iter()
                    .map(|row| {
                        (0..schema.fields().len())
                            .map(|index| {
                                row.try_get::<_, Option<String>>(index).map_err(|error| {
                                    SourceError::StartSnapshotFailed {
                                        reason: format!(
                                            "decode snapshot column {index} from '{table}': {error}"
                                        ),
                                    }
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let column_policies = metadata
                    .iter()
                    .map(|row| (row.get::<_, bool>(8), row.get::<_, bool>(9)))
                    .collect();
                snapshots.push(PgOutputSnapshotRelation {
                    relation,
                    column_policies,
                    rows,
                });
            }
            Ok::<_, SourceError>(PgOutputSourceSnapshot {
                lsn: PgLsn::parse(&snapshot_lsn)?,
                relations: snapshots,
            })
        }
        .await;
        let rollback = pgoutput.client.batch_execute("ROLLBACK").await;
        let snapshot = result?;
        rollback.map_err(|error| SourceError::StartSnapshotFailed {
            reason: format!("finish source-wide PostgreSQL snapshot: {error}"),
        })?;
        Ok(snapshot)
    }

    pub async fn relation_column_policies(
        &self,
        relation_id: u32,
    ) -> Result<Vec<(bool, bool)>, SourceError> {
        let pgoutput = self.pgoutput.as_ref().ok_or_else(|| {
            SourceError::Io("native pgoutput connection is unavailable".to_string())
        })?;
        pgoutput
            .client
            .query(
                "SELECT NOT attnotnull, atthasdef FROM pg_attribute WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
                &[&relation_id],
            )
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| (row.get::<_, bool>(0), row.get::<_, bool>(1)))
                    .collect()
            })
            .map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("inspect pgoutput relation column policy: {error}"),
            })
    }

    pub fn decode_and_enqueue(&mut self, payload: &[u8]) -> Result<(), SourceError> {
        if self.format == CdcWireFormat::PgOutput {
            if let Ok(frame) = std::str::from_utf8(payload) {
                let fields = frame.split('|').collect::<Vec<_>>();
                if fields.first() == Some(&"BEGIN") {
                    let xid = fields
                        .get(1)
                        .ok_or_else(|| SourceError::PollDeltaFailed {
                            reason: "pgoutput BEGIN is missing xid".to_string(),
                        })?
                        .parse()
                        .map_err(|_| SourceError::PollDeltaFailed {
                            reason: "pgoutput BEGIN has invalid xid".to_string(),
                        })?;
                    if self.transaction.is_some() {
                        return Err(SourceError::PollDeltaFailed {
                            reason: "pgoutput nested BEGIN is invalid".to_string(),
                        });
                    }
                    self.transaction = Some((xid, payload.len()));
                    self.transaction_changes.clear();
                    return Ok(());
                }
                if fields.first() == Some(&"COMMIT") {
                    let end_lsn = fields
                        .get(1)
                        .ok_or_else(|| SourceError::PollDeltaFailed {
                            reason: "pgoutput COMMIT is missing end LSN".to_string(),
                        })
                        .and_then(|lsn| PgLsn::parse(lsn))?;
                    let (xid, bytes) =
                        self.transaction
                            .take()
                            .ok_or_else(|| SourceError::PollDeltaFailed {
                                reason: "pgoutput COMMIT without BEGIN is invalid".to_string(),
                            })?;
                    let changes = std::mem::take(&mut self.transaction_changes);
                    let envelope = CdcTransactionEnvelope {
                        xid,
                        end_lsn,
                        changes,
                    };
                    self.last_decoded_envelope = Some(envelope.clone());
                    return self.enqueue_envelope(envelope, bytes + payload.len());
                }
            }
        }
        let change = match self.format {
            CdcWireFormat::PgOutput => decode_pgoutput(payload),
            CdcWireFormat::Wal2Json => decode_wal2json(payload),
        };
        match change {
            Ok(change) => {
                if let Some((_, bytes)) = &mut self.transaction {
                    *bytes = bytes.saturating_add(payload.len());
                    if *bytes > POSTGRES_CDC_MAX_TRANSACTION_BYTES {
                        self.replication_read_paused = true;
                        return Err(SourceError::PollDeltaFailed {
                            reason: "[RS-4014] pgoutput transaction exceeds POSTGRES_CDC_MAX_TRANSACTION_BYTES; replication is paused. Next steps: increase the bound or reduce upstream transaction size".to_string(),
                        });
                    }
                    self.transaction_changes.push(change);
                    Ok(())
                } else {
                    self.enqueue(change, payload.len())
                }
            }
            Err(err) => {
                let lsn = self.queued.back().map(|q| q.change.lsn).unwrap_or(PgLsn(0));
                rockstream_types::dlq::quarantine_record(
                    "postgres_cdc",
                    lsn,
                    "RS-1003",
                    &err.to_string(),
                    payload,
                );
                Ok(())
            }
        }
    }

    pub fn enqueue_envelope(
        &mut self,
        envelope: CdcTransactionEnvelope,
        payload_bytes: usize,
    ) -> Result<(), SourceError> {
        if payload_bytes > POSTGRES_CDC_MAX_TRANSACTION_BYTES {
            self.replication_read_paused = true;
            return Err(SourceError::PollDeltaFailed {
                reason: "[RS-4014] pgoutput transaction exceeds POSTGRES_CDC_MAX_TRANSACTION_BYTES; replication is paused. Next steps: increase the bound or reduce upstream transaction size".to_string(),
            });
        }
        let changes = envelope.changes;
        let change_count = changes.len();
        if self.queued.len().saturating_add(change_count) > POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS
            || self.queued_bytes.saturating_add(payload_bytes) > POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
        {
            self.replication_read_paused = true;
            return Err(SourceError::PollDeltaFailed {
                reason: format!(
                    "[RS-4014] pgoutput transaction cannot fit the bounded CDC queue ({}/{}, {}/{} bytes); replication is paused. Next steps: drain the source or increase the configured bound",
                    self.queued.len(),
                    POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS,
                    self.queued_bytes,
                    POSTGRES_CDC_MAX_IN_FLIGHT_BYTES,
                ),
            });
        }
        let per_change_bytes = payload_bytes / change_count.max(1);
        let remainder_bytes = payload_bytes % change_count.max(1);
        for (index, mut change) in changes.into_iter().enumerate() {
            if index + 1 == change_count {
                change.lsn = envelope.end_lsn;
            }
            self.enqueue_with_transaction_end(
                change,
                per_change_bytes + usize::from(index + 1 == change_count) * remainder_bytes,
                index + 1 == change_count,
            )?;
        }
        Ok(())
    }

    pub fn last_decoded_envelope(&self) -> Option<&CdcTransactionEnvelope> {
        self.last_decoded_envelope.as_ref()
    }

    pub fn enqueue(&mut self, change: CdcChange, payload_bytes: usize) -> Result<(), SourceError> {
        self.enqueue_with_transaction_end(change, payload_bytes, true)
    }

    fn enqueue_with_transaction_end(
        &mut self,
        change: CdcChange,
        payload_bytes: usize,
        transaction_end: bool,
    ) -> Result<(), SourceError> {
        if self.queued.len() >= POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS
            || self.queued_bytes.saturating_add(payload_bytes) > POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
        {
            self.replication_read_paused = true;
            return Err(SourceError::PollDeltaFailed {
                reason: format!(
                    "[RS-4014] PostgreSQL CDC buffer is full ({}/{} records, {}/{} bytes); replication is paused until credits drain. Next steps: drain the source or increase the configured bound",
                    self.queued.len(), POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS, self.queued_bytes,
                    POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
                ),
            });
        }
        self.queued_bytes += payload_bytes;
        self.queued.push_back(QueuedChange {
            change,
            bytes: payload_bytes,
            transaction_end,
        });
        Ok(())
    }

    pub fn queued_changes(&self) -> impl Iterator<Item = &CdcChange> {
        self.queued.iter().map(|queued| &queued.change)
    }

    pub fn buffered_records(&self) -> usize {
        self.queued.len()
    }

    pub fn buffered_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub fn buffer_fill_ratio(&self) -> f64 {
        let record_fill = self.queued.len() as f64 / POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS as f64;
        let byte_fill = self.queued_bytes as f64 / POSTGRES_CDC_MAX_IN_FLIGHT_BYTES as f64;
        record_fill.max(byte_fill)
    }

    pub fn transaction_buffer_fill_ratio(&self) -> f64 {
        let transaction_bytes = self.transaction.map_or(0, |(_, bytes)| bytes).max(
            self.native_transaction
                .as_ref()
                .map_or(0, |(_, _, bytes)| *bytes),
        );
        self.buffer_fill_ratio()
            .max(transaction_bytes as f64 / POSTGRES_CDC_MAX_TRANSACTION_BYTES as f64)
    }

    pub fn last_committed_lsn(&self) -> Option<PgLsn> {
        self.committed.map(|(_, lsn)| lsn)
    }

    /// `postgres_cdc_wal_lag_bytes` metric used by operators to detect a slow
    /// subscriber before it forces unbounded WAL retention on the primary.
    pub fn wal_lag_bytes(&self) -> u64 {
        self.wal_lag_bytes
    }

    pub fn set_wal_lag_bytes(&mut self, wal_lag_bytes: u64) {
        self.wal_lag_bytes = wal_lag_bytes;
        self.replication_read_paused = wal_lag_bytes >= POSTGRES_CDC_MAX_WAL_LAG_BYTES;
    }

    pub fn replication_read_paused(&self) -> bool {
        self.replication_read_paused
    }

    pub fn status(&self) -> &PostgresCdcStatus {
        &self.status
    }

    /// Classify recoverable replication failures without silently advancing an
    /// LSN.  The caller may retry through `begin_resnapshot` at most three
    /// times, after which this remains a documented BLOCKED state.
    pub fn mark_failure(&mut self, failure: PostgresCdcFailure) {
        let reason = match failure {
            PostgresCdcFailure::SlotInvalidated => "replication slot was invalidated",
            PostgresCdcFailure::PublicationMissing => "publication is missing",
            PostgresCdcFailure::ReplicationTimeout => "replication connection timed out",
            PostgresCdcFailure::WalRetentionLost => "required WAL has been removed",
        };
        self.status = PostgresCdcStatus::Blocked {
            code: "RS-4011",
            reason: format!("{reason}. Next steps: repair PostgreSQL replication settings, then resume the source"),
        };
        self.replication_read_paused = true;
    }

    pub fn begin_resnapshot(&mut self) -> Result<(), SourceError> {
        if !matches!(&self.status, PostgresCdcStatus::Blocked { .. }) {
            return Err(SourceError::Io(
                "resnapshot is allowed only from BLOCKED PostgreSQL CDC state".to_string(),
            ));
        }
        if self.recovery_attempts >= POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS {
            return Err(SourceError::Io(format!(
                "[RS-4011] postgres_cdc.resnapshot_exhausted: {} automatic resnapshot attempts failed. Next steps: repair the source and issue ALTER SOURCE ... RESUME",
                POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS
            )));
        }
        self.recovery_attempts += 1;
        self.status = PostgresCdcStatus::Resnapshotting {
            attempt: self.recovery_attempts,
        };
        Ok(())
    }

    pub fn complete_resnapshot(&mut self) {
        self.status = PostgresCdcStatus::Running;
        self.replication_read_paused = false;
    }

    fn batch_for(changes: &[CdcChange], schema: SchemaRef) -> Result<RecordBatch, SourceError> {
        let mut rows = Vec::new();
        let mut weights = Vec::new();
        for change in changes {
            match change.operation {
                CdcOperation::Insert => {
                    rows.push(change.new_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "INSERT change is missing new values".to_string(),
                        }
                    })?);
                    weights.push(1);
                }
                CdcOperation::Delete => {
                    rows.push(change.old_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "DELETE change is missing old values".to_string(),
                        }
                    })?);
                    weights.push(-1);
                }
                CdcOperation::Update => {
                    rows.push(change.old_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "UPDATE change is missing old values".to_string(),
                        }
                    })?);
                    weights.push(-1);
                    rows.push(change.new_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "UPDATE change is missing new values".to_string(),
                        }
                    })?);
                    weights.push(1);
                }
            }
        }
        let mut columns: Vec<Vec<i64>> =
            vec![Vec::with_capacity(rows.len()); schema.fields().len()];
        for row in rows {
            if row.len() != columns.len() {
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "CDC row has {} columns but source schema has {}",
                        row.len(),
                        columns.len()
                    ),
                });
            }
            for (index, value) in row.into_iter().enumerate() {
                columns[index].push(value);
            }
        }
        let arrays: Vec<ArrayRef> = columns
            .into_iter()
            .zip(schema.fields())
            .map(|(values, field)| match field.data_type() {
                DataType::Int64 => Ok(Arc::new(Int64Array::from(values)) as ArrayRef),
                DataType::Int32 => values
                    .into_iter()
                    .map(|value| {
                        i32::try_from(value).map_err(|_| SourceError::PollDeltaFailed {
                            reason: format!("pgoutput value {value} does not fit INT"),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|values| Arc::new(arrow::array::Int32Array::from(values)) as ArrayRef),
                data_type => Err(SourceError::PollDeltaFailed {
                    reason: format!("native pgoutput does not support {data_type} columns"),
                }),
            })
            .collect::<Result<_, _>>()?;
        let batch =
            RecordBatch::try_new(schema, arrays).map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("failed to build CDC record batch: {error}"),
            })?;
        append_weight_column(batch, &weights).map_err(|error| SourceError::PollDeltaFailed {
            reason: format!("failed to append CDC weights: {error}"),
        })
    }

    fn pgoutput_text_batch(
        changes: &[PgOutputTextChange],
        schema: SchemaRef,
    ) -> Result<RecordBatch, SourceError> {
        let mut rows = Vec::new();
        let mut weights = Vec::new();
        for change in changes {
            match change.operation {
                CdcOperation::Insert => {
                    rows.push(change.new_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "pgoutput INSERT is missing values".to_string(),
                        }
                    })?);
                    weights.push(1);
                }
                CdcOperation::Delete => {
                    rows.push(change.old_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "pgoutput DELETE is missing REPLICA IDENTITY FULL values"
                                .to_string(),
                        }
                    })?);
                    weights.push(-1);
                }
                CdcOperation::Update => {
                    rows.push(change.old_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "pgoutput UPDATE is missing old values".to_string(),
                        }
                    })?);
                    rows.push(change.new_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "pgoutput UPDATE is missing new values".to_string(),
                        }
                    })?);
                    weights.extend([-1, 1]);
                }
            }
        }
        if rows.iter().any(|row| row.len() != schema.fields().len()) {
            return Err(SourceError::PollDeltaFailed {
                reason: "pgoutput tuple width differs from the bound source schema".to_string(),
            });
        }
        let arrays = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let values = rows
                    .iter()
                    .map(|row| row[index].as_str())
                    .collect::<Vec<_>>();
                match field.data_type() {
                    DataType::Int64 => values
                        .into_iter()
                        .map(|value| {
                            value.parse().map_err(|_| SourceError::PollDeltaFailed {
                                reason: format!("pgoutput value '{value}' is not BIGINT"),
                            })
                        })
                        .collect::<Result<Vec<i64>, _>>()
                        .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef),
                    DataType::Int32 => values
                        .into_iter()
                        .map(|value| {
                            value.parse().map_err(|_| SourceError::PollDeltaFailed {
                                reason: format!("pgoutput value '{value}' is not INT"),
                            })
                        })
                        .collect::<Result<Vec<i32>, _>>()
                        .map(|values| Arc::new(Int32Array::from(values)) as ArrayRef),
                    DataType::Utf8 => Ok(Arc::new(StringArray::from(values)) as ArrayRef),
                    DataType::Decimal128(precision, scale) => values
                        .into_iter()
                        .map(|value| decimal_scaled(value, *scale))
                        .collect::<Result<Vec<i128>, _>>()
                        .and_then(|values| {
                            Decimal128Array::from(values)
                                .with_precision_and_scale(*precision, *scale)
                                .map(|array| Arc::new(array) as ArrayRef)
                                .map_err(|error| SourceError::PollDeltaFailed {
                                    reason: format!("pgoutput DECIMAL: {error}"),
                                })
                        }),
                    data_type => Err(SourceError::PollDeltaFailed {
                        reason: format!("native pgoutput does not support {data_type} columns"),
                    }),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch =
            RecordBatch::try_new(schema, arrays).map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("build pgoutput batch: {error}"),
            })?;
        append_weight_column(batch, &weights).map_err(|error| SourceError::PollDeltaFailed {
            reason: format!("add pgoutput weights: {error}"),
        })
    }

    async fn capture_pgoutput_snapshot(&mut self) -> Result<SnapshotDeltaFence, SourceError> {
        let pgoutput = self.pgoutput.as_mut().ok_or_else(|| {
            SourceError::Io("native pgoutput connection is unavailable".to_string())
        })?;
        let existing = pgoutput
            .client
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&pgoutput.slot],
            )
            .await
            .map_err(|error| SourceError::StartSnapshotFailed {
                reason: format!("inspect pgoutput slot: {error}"),
            })?;
        if existing.is_none() {
            pgoutput
                .client
                .query_one(
                    "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&pgoutput.slot],
                )
                .await
                .map_err(|error| SourceError::StartSnapshotFailed {
                    reason: format!("create pgoutput slot '{}': {error}", pgoutput.slot),
                })?;
        }
        pgoutput
            .client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|error| SourceError::StartSnapshotFailed {
                reason: format!("begin PostgreSQL snapshot: {error}"),
            })?;
        let result = async {
            let snapshot_lsn: String = pgoutput
                .client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await
                .map_err(|error| SourceError::StartSnapshotFailed {
                    reason: format!("capture PostgreSQL snapshot LSN: {error}"),
                })?
                .get(0);
            let projection = self
                .schema
                .fields()
                .iter()
                .map(|field| format!("\"{}\"::text", field.name().replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ");
            let rows = pgoutput
                .client
                .query(&format!("SELECT {projection} FROM {}", pgoutput.table), &[])
                .await
                .map_err(|error| SourceError::StartSnapshotFailed {
                    reason: format!("read PostgreSQL snapshot: {error}"),
                })?;
            let rows = rows
                .into_iter()
                .map(|row| {
                    (0..self.schema.fields().len())
                        .map(|index| {
                            row.try_get::<_, String>(index).map_err(|error| {
                                SourceError::StartSnapshotFailed {
                                    reason: format!(
                                        "decode PostgreSQL snapshot column {index}: {error}"
                                    ),
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, SourceError>((PgLsn::parse(&snapshot_lsn)?, rows))
        }
        .await;
        let rollback = pgoutput.client.batch_execute("ROLLBACK").await;
        let (snapshot_lsn, rows) = result?;
        rollback.map_err(|error| SourceError::StartSnapshotFailed {
            reason: format!("finish PostgreSQL snapshot: {error}"),
        })?;
        let batch = Self::pgoutput_text_batch(
            &rows
                .into_iter()
                .map(|new_values| PgOutputTextChange {
                    operation: CdcOperation::Insert,
                    old_values: None,
                    new_values: Some(new_values),
                })
                .collect::<Vec<_>>(),
            self.schema.clone(),
        )
        .map_err(|error| SourceError::StartSnapshotFailed {
            reason: error.to_string(),
        })?;
        self.snapshot_batches = if batch.num_rows() > 0 {
            vec![batch]
        } else {
            Vec::new()
        };
        pgoutput.snapshot_lsn = Some(snapshot_lsn);
        Ok(SnapshotDeltaFence::new(
            snapshot_lsn.to_offset_token(),
            snapshot_lsn.to_offset_token(),
        ))
    }

    async fn poll_pgoutput(
        &mut self,
        after: PgLsn,
        max_bytes: usize,
        credits_available: usize,
    ) -> Result<PollDeltaResult, SourceError> {
        if credits_available == 0 || max_bytes == 0 {
            return Ok(PollDeltaResult {
                batches: Vec::new(),
                new_offset: after.to_offset_token(),
                watermark: None,
            });
        }
        let pgoutput = self.pgoutput.as_mut().ok_or_else(|| {
            SourceError::Io("native pgoutput connection is unavailable".to_string())
        })?;
        let limit = i32::try_from(credits_available.saturating_mul(4)).unwrap_or(i32::MAX);
        let messages = pgoutput
            .client
            .query(
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)",
                &[&pgoutput.slot, &limit, &pgoutput.publication],
            )
            .await
            .map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("peek pgoutput changes: {error}"),
            })?;
        for message in messages.into_iter().skip(self.native_seen_messages) {
            let lsn = PgLsn::parse(message.get::<_, String>(0).as_str())?;
            let payload = message.get::<_, Vec<u8>>(1);
            self.native_seen_messages += 1;
            match payload.first() {
                Some(b'B') => {
                    if self.native_transaction.is_some() {
                        return Err(SourceError::PollDeltaFailed {
                            reason: "pgoutput nested BEGIN is invalid".to_string(),
                        });
                    }
                    self.native_transaction =
                        Some((pgoutput_begin_xid(&payload)?, Vec::new(), payload.len()));
                    continue;
                }
                Some(b'C') => {
                    let end_lsn = pgoutput_commit_lsn(&payload)?;
                    let (_, changes, bytes) = self.native_transaction.take().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "pgoutput COMMIT without BEGIN is invalid".to_string(),
                        }
                    })?;
                    if bytes.saturating_add(payload.len()) > POSTGRES_CDC_MAX_TRANSACTION_BYTES {
                        self.replication_read_paused = true;
                        return Err(SourceError::PollDeltaFailed { reason: "[RS-4014] pgoutput transaction exceeds POSTGRES_CDC_MAX_TRANSACTION_BYTES; replication is paused. Next steps: increase the bound or reduce upstream transaction size".to_string() });
                    }
                    if end_lsn <= after {
                        continue;
                    }
                    self.native_seen_messages = 0;
                    return Ok(PollDeltaResult {
                        batches: vec![Self::pgoutput_text_batch(&changes, self.schema.clone())?],
                        new_offset: end_lsn.to_offset_token(),
                        watermark: Some(end_lsn.0),
                    });
                }
                _ => {}
            }
            if let Some(change) = decode_native_pgoutput_text_message(
                &mut pgoutput.relations,
                self.schema.fields().len(),
                lsn,
                &payload,
            )? {
                let Some((_, changes, bytes)) = &mut self.native_transaction else {
                    return Err(SourceError::PollDeltaFailed {
                        reason: "pgoutput row frame without BEGIN is invalid".to_string(),
                    });
                };
                *bytes = bytes.saturating_add(payload.len());
                if *bytes > POSTGRES_CDC_MAX_TRANSACTION_BYTES {
                    self.replication_read_paused = true;
                    return Err(SourceError::PollDeltaFailed { reason: "[RS-4014] pgoutput transaction exceeds POSTGRES_CDC_MAX_TRANSACTION_BYTES; replication is paused. Next steps: increase the bound or reduce upstream transaction size".to_string() });
                }
                changes.push(change);
            }
        }
        Ok(PollDeltaResult {
            batches: Vec::new(),
            new_offset: after.to_offset_token(),
            watermark: None,
        })
    }

    /// Read one relation-aware protocol event without advancing the slot.
    /// The gateway coordinator is the only caller and does not request the
    /// next transaction until the current commit has been durably acknowledged.
    pub async fn poll_pgoutput_event(
        &mut self,
        max_messages: usize,
    ) -> Result<Option<PgOutputEvent>, SourceError> {
        if self.manually_paused {
            return Err(SourceError::Io(
                "source is paused; call resume before polling".to_string(),
            ));
        }
        let pgoutput = self.pgoutput.as_mut().ok_or_else(|| {
            SourceError::Io("native pgoutput connection is unavailable".to_string())
        })?;
        let limit = i32::try_from(
            self.native_seen_messages
                .saturating_add(max_messages.max(1)),
        )
        .unwrap_or(i32::MAX);
        let messages = pgoutput
            .client
            .query(
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)",
                &[&pgoutput.slot, &limit, &pgoutput.publication],
            )
            .await
            .map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("peek pgoutput event: {error}"),
            })?;
        let Some(message) = messages.into_iter().nth(self.native_seen_messages) else {
            return Ok(None);
        };
        let payload = message.get::<_, Vec<u8>>(1);
        self.native_seen_messages = self.native_seen_messages.saturating_add(1);
        let event = decode_pgoutput_event(&mut self.native_xid, &payload)?;
        if matches!(event, PgOutputEvent::Commit { .. }) {
            self.native_seen_messages = 0;
        }
        Ok(Some(event))
    }
}

#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    async fn capture_snapshot_delta_fence(
        &mut self,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        if self.pgoutput.is_none() && self.pgoutput_config.is_some() {
            self.open_pgoutput(false).await?;
        }
        if self.pgoutput.is_some() {
            return self.capture_pgoutput_snapshot().await;
        }
        let snapshot = self
            .committed
            .map(|(_, lsn)| lsn.to_offset_token())
            .unwrap_or_else(|| PgLsn(0).to_offset_token());
        let live = self
            .queued
            .back()
            .map(|queued| queued.change.lsn.to_offset_token())
            .unwrap_or_else(|| snapshot.clone());
        Ok(SnapshotDeltaFence::new(snapshot, live))
    }

    async fn start_snapshot(
        &mut self,
        _fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        let start = after
            .as_ref()
            .and_then(|offset| std::str::from_utf8(offset.as_bytes()).ok())
            .and_then(|offset| offset.strip_prefix("snapshot:"))
            .and_then(|index| index.parse::<usize>().ok())
            .unwrap_or(0);
        Ok(SnapshotStream::new(
            self.snapshot_batches
                .iter()
                .skip(start)
                .cloned()
                .enumerate()
                .map(|(index, batch)| SnapshotBatch {
                    batch,
                    resume_offset: OffsetToken::new(
                        format!("snapshot:{}", start + index + 1).into_bytes(),
                    ),
                })
                .collect(),
        ))
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        self.apply_secret_token_at_epoch();
        if self.pgoutput.is_none() && self.pgoutput_config.is_some() {
            self.open_pgoutput(false).await?;
        }
        if self.pgoutput.is_some() {
            return self
                .poll_pgoutput(
                    PgLsn::from_offset_token(&after)?,
                    max_bytes,
                    credits_available,
                )
                .await;
        }
        let after = PgLsn::from_offset_token(&after)?;
        if credits_available == 0 || max_bytes == 0 {
            return Ok(PollDeltaResult {
                batches: Vec::new(),
                new_offset: after.to_offset_token(),
                watermark: None,
            });
        }
        if self.manually_paused {
            return Err(SourceError::Io(
                "source is paused; call resume before polling".to_string(),
            ));
        }
        let allowance = credits_available.min(POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS);
        let mut records = 0;
        let mut changes = Vec::new();
        while !self.queued.is_empty() {
            let transaction_len = self
                .queued
                .iter()
                .take_while(|queued| !queued.transaction_end)
                .count()
                + 1;
            let transaction_bytes = self
                .queued
                .iter()
                .take(transaction_len)
                .map(|queued| queued.bytes)
                .sum::<usize>();
            if transaction_len > allowance.saturating_sub(records) || transaction_bytes > max_bytes
            {
                if changes.is_empty() {
                    return Err(SourceError::PollDeltaFailed {
                        reason: "[RS-4014] pgoutput transaction exceeds poll credits or byte budget; replication remains paused. Next steps: raise the source epoch budget".to_string(),
                    });
                }
                break;
            }
            for _ in 0..transaction_len {
                let Some(queued) = self.queued.pop_front() else {
                    break;
                };
                self.queued_bytes = self.queued_bytes.saturating_sub(queued.bytes);
                if queued.change.lsn > after {
                    changes.push(queued.change);
                }
            }
            records += transaction_len;
        }
        if changes.is_empty() {
            return Ok(PollDeltaResult {
                batches: Vec::new(),
                new_offset: after.to_offset_token(),
                watermark: None,
            });
        }
        if self.queued.len() < POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS
            && self.queued_bytes < POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
            && self.wal_lag_bytes < POSTGRES_CDC_MAX_WAL_LAG_BYTES
        {
            self.replication_read_paused = false;
        }
        let last_lsn = match changes.last() {
            Some(c) => c.lsn,
            None => {
                return Ok(PollDeltaResult {
                    batches: Vec::new(),
                    new_offset: after.to_offset_token(),
                    watermark: None,
                });
            }
        };

        let batch = Self::batch_for(&changes, self.schema.clone())?;
        Ok(PollDeltaResult {
            batches: vec![batch],
            new_offset: last_lsn.to_offset_token(),
            watermark: Some(last_lsn.0),
        })
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        self.apply_secret_token_at_epoch();
        let lsn = PgLsn::from_offset_token(&offset)?;
        if self.pgoutput.is_none() && self.pgoutput_config.is_some() {
            return Err(SourceError::CommitOffsetFailed {
                epoch,
                reason: "pgoutput connection is closed or fenced".to_string(),
            });
        }
        if let Some(pgoutput) = &mut self.pgoutput {
            if lsn != PgLsn::ZERO {
                let target = lsn.to_string();
                pgoutput
                    .client
                    .query(
                        "SELECT lsn FROM pg_logical_slot_get_binary_changes($1, $2::text::pg_lsn, NULL, 'proto_version', '1', 'publication_names', $3)",
                        &[&pgoutput.slot, &target, &pgoutput.publication],
                    )
                    .await
                    .map_err(|error| SourceError::CommitOffsetFailed {
                        epoch,
                        reason: format!("advance pgoutput slot: {error}"),
                    })?;
            }
        }
        self.committed = Some((epoch, lsn));
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.manually_paused = true;
        self.close_pgoutput();
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.manually_paused = false;
        Ok(())
    }

    fn watermark_capability(&self) -> WatermarkCapability {
        WatermarkCapability::Native
    }
}

fn quote_relation(relation: &str) -> Result<String, SourceError> {
    let quoted = relation
        .split('.')
        .map(|part| {
            (!part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'))
            .then(|| format!("\"{part}\""))
            .ok_or_else(|| SourceError::DiscoverSchemaFailed {
                reason: format!("invalid PostgreSQL source table '{relation}'"),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    (!quoted.is_empty() && quoted.len() <= 2)
        .then(|| quoted.join("."))
        .ok_or_else(|| SourceError::DiscoverSchemaFailed {
            reason: format!("invalid PostgreSQL source table '{relation}'"),
        })
}

#[cfg(test)]
fn decode_native_pgoutput_message(
    relations: &mut std::collections::HashMap<u32, PgOutputRelation>,
    schema_columns: usize,
    lsn: PgLsn,
    payload: &[u8],
) -> Result<Option<CdcChange>, SourceError> {
    let Some((&tag, mut data)) = payload.split_first() else {
        return Err(SourceError::PollDeltaFailed {
            reason: "empty pgoutput message".to_string(),
        });
    };
    match tag {
        b'B' | b'C' | b'O' | b'Y' | b'T' => Ok(None),
        b'R' => {
            let relation_id = take_u32(&mut data)?;
            take_cstring(&mut data)?;
            take_cstring(&mut data)?;
            take_byte(&mut data)?;
            let columns = usize::from(take_u16(&mut data)?);
            for _ in 0..columns {
                take_byte(&mut data)?;
                take_cstring(&mut data)?;
                take_u32(&mut data)?;
                take_u32(&mut data)?;
            }
            if columns != schema_columns {
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "pgoutput relation {relation_id} has {columns} columns but bound source schema has {schema_columns}"
                    ),
                });
            }
            relations.insert(relation_id, PgOutputRelation { columns });
            Ok(None)
        }
        b'I' => {
            let relation_id = take_u32(&mut data)?;
            ensure_relation(relations, relation_id, schema_columns)?;
            expect_byte(&mut data, b'N')?;
            let new_values = parse_pgoutput_tuple(&mut data, schema_columns)?;
            Ok(Some(native_change(
                lsn,
                relation_id,
                CdcOperation::Insert,
                None,
                Some(new_values),
            )))
        }
        b'U' => {
            let relation_id = take_u32(&mut data)?;
            ensure_relation(relations, relation_id, schema_columns)?;
            let old_tag = take_byte(&mut data)?;
            if old_tag != b'O' {
                return Err(SourceError::PollDeltaFailed {
                    reason: "pgoutput UPDATE requires REPLICA IDENTITY FULL to persist a delete preimage".to_string(),
                });
            }
            let old_values = parse_pgoutput_tuple(&mut data, schema_columns)?;
            expect_byte(&mut data, b'N')?;
            let new_values = parse_pgoutput_tuple(&mut data, schema_columns)?;
            Ok(Some(native_change(
                lsn,
                relation_id,
                CdcOperation::Update,
                Some(old_values),
                Some(new_values),
            )))
        }
        b'D' => {
            let relation_id = take_u32(&mut data)?;
            ensure_relation(relations, relation_id, schema_columns)?;
            if take_byte(&mut data)? != b'O' {
                return Err(SourceError::PollDeltaFailed {
                    reason: "pgoutput DELETE requires REPLICA IDENTITY FULL to persist a delete preimage".to_string(),
                });
            }
            let old_values = parse_pgoutput_tuple(&mut data, schema_columns)?;
            Ok(Some(native_change(
                lsn,
                relation_id,
                CdcOperation::Delete,
                Some(old_values),
                None,
            )))
        }
        _ => Ok(None),
    }
}

fn decode_native_pgoutput_text_message(
    relations: &mut std::collections::HashMap<u32, PgOutputRelation>,
    schema_columns: usize,
    _lsn: PgLsn,
    payload: &[u8],
) -> Result<Option<PgOutputTextChange>, SourceError> {
    let Some((&tag, mut data)) = payload.split_first() else {
        return Err(SourceError::PollDeltaFailed {
            reason: "empty pgoutput message".to_string(),
        });
    };
    match tag {
        b'B' | b'C' | b'O' | b'Y' | b'T' => Ok(None),
        b'R' => {
            let relation_id = take_u32(&mut data)?;
            take_cstring(&mut data)?;
            take_cstring(&mut data)?;
            take_byte(&mut data)?;
            let columns = usize::from(take_u16(&mut data)?);
            for _ in 0..columns {
                take_byte(&mut data)?;
                take_cstring(&mut data)?;
                take_u32(&mut data)?;
                take_u32(&mut data)?;
            }
            if columns != schema_columns {
                return Err(SourceError::PollDeltaFailed { reason: format!("pgoutput relation {relation_id} has {columns} columns but bound source schema has {schema_columns}") });
            }
            relations.insert(relation_id, PgOutputRelation { columns });
            Ok(None)
        }
        b'I' => {
            let relation = take_u32(&mut data)?;
            ensure_relation(relations, relation, schema_columns)?;
            expect_byte(&mut data, b'N')?;
            Ok(Some(PgOutputTextChange {
                operation: CdcOperation::Insert,
                old_values: None,
                new_values: Some(parse_pgoutput_text_tuple(&mut data, schema_columns)?),
            }))
        }
        b'U' => {
            let relation = take_u32(&mut data)?;
            ensure_relation(relations, relation, schema_columns)?;
            if take_byte(&mut data)? != b'O' {
                return Err(SourceError::PollDeltaFailed { reason: "pgoutput UPDATE requires REPLICA IDENTITY FULL to persist a delete preimage".to_string() });
            }
            let old_values = parse_pgoutput_text_tuple(&mut data, schema_columns)?;
            expect_byte(&mut data, b'N')?;
            Ok(Some(PgOutputTextChange {
                operation: CdcOperation::Update,
                old_values: Some(old_values),
                new_values: Some(parse_pgoutput_text_tuple(&mut data, schema_columns)?),
            }))
        }
        b'D' => {
            let relation = take_u32(&mut data)?;
            ensure_relation(relations, relation, schema_columns)?;
            if take_byte(&mut data)? != b'O' {
                return Err(SourceError::PollDeltaFailed { reason: "pgoutput DELETE requires REPLICA IDENTITY FULL to persist a delete preimage".to_string() });
            }
            Ok(Some(PgOutputTextChange {
                operation: CdcOperation::Delete,
                old_values: Some(parse_pgoutput_text_tuple(&mut data, schema_columns)?),
                new_values: None,
            }))
        }
        _ => Ok(None),
    }
}

fn decode_pgoutput_event(
    active_xid: &mut Option<u32>,
    payload: &[u8],
) -> Result<PgOutputEvent, SourceError> {
    let Some((&tag, mut data)) = payload.split_first() else {
        return Err(SourceError::PollDeltaFailed {
            reason: "empty pgoutput message".to_string(),
        });
    };
    if tag == b'B' {
        if active_xid.is_some() {
            return Err(pgoutput_protocol_error("nested BEGIN"));
        }
        let xid = pgoutput_begin_xid(payload)?;
        *active_xid = Some(xid);
        return Ok(PgOutputEvent::Begin { xid });
    }
    let xid = active_xid.ok_or_else(|| pgoutput_protocol_error("message without BEGIN"))?;
    match tag {
        b'C' => {
            let commit_lsn = pgoutput_commit_lsn(payload)?;
            *active_xid = None;
            Ok(PgOutputEvent::Commit { xid, commit_lsn })
        }
        b'R' => {
            let relation_id = take_u32(&mut data)?;
            let namespace = cstring_to_string(take_cstring(&mut data)?)?;
            let name = cstring_to_string(take_cstring(&mut data)?)?;
            let replica_identity = take_byte(&mut data)?;
            let column_count = usize::from(take_u16(&mut data)?);
            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                columns.push(PgOutputColumn {
                    flags: take_byte(&mut data)?,
                    name: cstring_to_string(take_cstring(&mut data)?)?,
                    type_oid: take_u32(&mut data)?,
                    type_modifier: take_u32(&mut data)? as i32,
                });
            }
            if !data.is_empty() {
                return Err(pgoutput_protocol_error("malformed Relation frame"));
            }
            Ok(PgOutputEvent::Relation {
                xid,
                relation: PgOutputRelationMetadata {
                    relation_id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                },
            })
        }
        b'I' => {
            let relation_id = take_u32(&mut data)?;
            expect_byte(&mut data, b'N')?;
            let new_values = parse_pgoutput_nullable_text_tuple(&mut data)?;
            ensure_consumed(data, "Insert")?;
            Ok(PgOutputEvent::Insert {
                xid,
                relation_id,
                new_values,
            })
        }
        b'U' => {
            let relation_id = take_u32(&mut data)?;
            if take_byte(&mut data)? != b'O' {
                return Err(pgoutput_protocol_error(
                    "UPDATE requires REPLICA IDENTITY FULL",
                ));
            }
            let old_values = parse_pgoutput_nullable_text_tuple(&mut data)?;
            expect_byte(&mut data, b'N')?;
            let new_values = parse_pgoutput_nullable_text_tuple(&mut data)?;
            ensure_consumed(data, "Update")?;
            Ok(PgOutputEvent::Update {
                xid,
                relation_id,
                old_values,
                new_values,
            })
        }
        b'D' => {
            let relation_id = take_u32(&mut data)?;
            if take_byte(&mut data)? != b'O' {
                return Err(pgoutput_protocol_error(
                    "DELETE requires REPLICA IDENTITY FULL",
                ));
            }
            let old_values = parse_pgoutput_nullable_text_tuple(&mut data)?;
            ensure_consumed(data, "Delete")?;
            Ok(PgOutputEvent::Delete {
                xid,
                relation_id,
                old_values,
            })
        }
        _ => Err(pgoutput_protocol_error(&format!(
            "unsupported message tag 0x{tag:02x}"
        ))),
    }
}

fn parse_pgoutput_nullable_text_tuple(
    data: &mut &[u8],
) -> Result<Vec<Option<String>>, SourceError> {
    let count = usize::from(take_u16(data)?);
    (0..count)
        .map(|_| match take_byte(data)? {
            b'n' => Ok(None),
            b't' => {
                let length = usize::try_from(take_i32(data)?)
                    .map_err(|_| pgoutput_protocol_error("tuple has a negative value length"))?;
                std::str::from_utf8(take_bytes(data, length)?)
                    .map(|value| Some(value.to_string()))
                    .map_err(|_| pgoutput_protocol_error("tuple text is not UTF-8"))
            }
            b'u' => Err(pgoutput_protocol_error(
                "unchanged TOAST values are unsupported",
            )),
            _ => Err(pgoutput_protocol_error(
                "binary or unknown tuple value is unsupported",
            )),
        })
        .collect()
}

fn cstring_to_string(bytes: &[u8]) -> Result<String, SourceError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| pgoutput_protocol_error("identifier is not UTF-8"))
}

fn ensure_consumed(data: &[u8], frame: &str) -> Result<(), SourceError> {
    if data.is_empty() {
        Ok(())
    } else {
        Err(pgoutput_protocol_error(&format!("malformed {frame} frame")))
    }
}

fn pgoutput_protocol_error(detail: &str) -> SourceError {
    SourceError::PollDeltaFailed {
        reason: format!("RS-4013: pgoutput protocol error: {detail}"),
    }
}

#[cfg(test)]
fn native_change(
    lsn: PgLsn,
    table_id: u32,
    operation: CdcOperation,
    old_values: Option<Vec<i64>>,
    new_values: Option<Vec<i64>>,
) -> CdcChange {
    let key = old_values
        .as_ref()
        .or(new_values.as_ref())
        .and_then(|values| values.first())
        .map(|value| value.to_be_bytes().to_vec())
        .unwrap_or_default();
    CdcChange {
        lsn,
        table_id,
        row_id: CdcChange::row_id_for(table_id, &key),
        primary_key: key,
        operation,
        old_values,
        new_values,
    }
}

fn ensure_relation(
    relations: &std::collections::HashMap<u32, PgOutputRelation>,
    relation_id: u32,
    schema_columns: usize,
) -> Result<(), SourceError> {
    relations
        .get(&relation_id)
        .filter(|relation| relation.columns == schema_columns)
        .map(|_| ())
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!(
                "pgoutput relation metadata for relation {relation_id} was not received"
            ),
        })
}

#[cfg(test)]
fn parse_pgoutput_tuple(data: &mut &[u8], columns: usize) -> Result<Vec<i64>, SourceError> {
    let count = usize::from(take_u16(data)?);
    if count != columns {
        return Err(SourceError::PollDeltaFailed {
            reason: format!("pgoutput tuple has {count} columns but source schema has {columns}"),
        });
    }
    (0..count)
        .map(|_| {
            if take_byte(data)? != b't' {
                return Err(SourceError::PollDeltaFailed {
                    reason: "pgoutput source requires non-NULL text-format BIGINT tuple values"
                        .to_string(),
                });
            }
            let length =
                usize::try_from(take_i32(data)?).map_err(|_| SourceError::PollDeltaFailed {
                    reason: "pgoutput tuple has a negative value length".to_string(),
                })?;
            let bytes = take_bytes(data, length)?;
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|value| value.parse().ok())
                .ok_or_else(|| SourceError::PollDeltaFailed {
                    reason: "pgoutput tuple value is not a BIGINT".to_string(),
                })
        })
        .collect()
}

fn parse_pgoutput_text_tuple(data: &mut &[u8], columns: usize) -> Result<Vec<String>, SourceError> {
    let count = usize::from(take_u16(data)?);
    if count != columns {
        return Err(SourceError::PollDeltaFailed {
            reason: format!("pgoutput tuple has {count} columns but source schema has {columns}"),
        });
    }
    (0..count)
        .map(|_| {
            if take_byte(data)? != b't' {
                return Err(SourceError::PollDeltaFailed {
                    reason: "pgoutput source requires non-NULL text tuple values".to_string(),
                });
            }
            let length =
                usize::try_from(take_i32(data)?).map_err(|_| SourceError::PollDeltaFailed {
                    reason: "pgoutput tuple has a negative value length".to_string(),
                })?;
            std::str::from_utf8(take_bytes(data, length)?)
                .map(str::to_string)
                .map_err(|_| SourceError::PollDeltaFailed {
                    reason: "pgoutput tuple is not UTF-8".to_string(),
                })
        })
        .collect()
}

fn decimal_scaled(value: &str, scale: i8) -> Result<i128, SourceError> {
    let negative = value.starts_with('-');
    let value = value.trim_start_matches(['-', '+']);
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    let scale = usize::try_from(scale).map_err(|_| SourceError::PollDeltaFailed {
        reason: "negative DECIMAL scale is unsupported".to_string(),
    })?;
    if fraction.len() > scale
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(SourceError::PollDeltaFailed {
            reason: format!("pgoutput value '{value}' is not a compatible DECIMAL"),
        });
    }
    let digits = format!("{whole}{fraction:0<scale$}");
    let scaled = digits
        .parse::<i128>()
        .map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("pgoutput DECIMAL '{value}' overflows"),
        })?;
    Ok(if negative { -scaled } else { scaled })
}

fn take_byte(data: &mut &[u8]) -> Result<u8, SourceError> {
    let Some((&byte, rest)) = data.split_first() else {
        return Err(SourceError::PollDeltaFailed {
            reason: "truncated pgoutput message".to_string(),
        });
    };
    *data = rest;
    Ok(byte)
}

fn expect_byte(data: &mut &[u8], expected: u8) -> Result<(), SourceError> {
    (take_byte(data)? == expected)
        .then_some(())
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!(
                "invalid pgoutput tuple marker; expected '{}",
                expected as char
            ),
        })
}

fn take_bytes<'a>(data: &mut &'a [u8], length: usize) -> Result<&'a [u8], SourceError> {
    if data.len() < length {
        return Err(SourceError::PollDeltaFailed {
            reason: "truncated pgoutput message".to_string(),
        });
    }
    let (value, rest) = data.split_at(length);
    *data = rest;
    Ok(value)
}

fn take_u16(data: &mut &[u8]) -> Result<u16, SourceError> {
    Ok(u16::from_be_bytes(
        take_bytes(data, 2)?
            .try_into()
            .map_err(|_| SourceError::PollDeltaFailed {
                reason: "truncated pgoutput message".to_string(),
            })?,
    ))
}

fn take_u32(data: &mut &[u8]) -> Result<u32, SourceError> {
    Ok(u32::from_be_bytes(
        take_bytes(data, 4)?
            .try_into()
            .map_err(|_| SourceError::PollDeltaFailed {
                reason: "truncated pgoutput message".to_string(),
            })?,
    ))
}

fn pgoutput_begin_xid(payload: &[u8]) -> Result<u32, SourceError> {
    let Some((&tag, mut data)) = payload.split_first() else {
        return Err(SourceError::PollDeltaFailed {
            reason: "empty pgoutput message".to_string(),
        });
    };
    if tag != b'B' {
        return Err(SourceError::PollDeltaFailed {
            reason: "expected pgoutput BEGIN frame".to_string(),
        });
    }
    take_bytes(&mut data, 16)?;
    let xid = take_u32(&mut data)?;
    if !data.is_empty() {
        return Err(SourceError::PollDeltaFailed {
            reason: "malformed pgoutput BEGIN frame".to_string(),
        });
    }
    Ok(xid)
}

fn pgoutput_commit_lsn(payload: &[u8]) -> Result<PgLsn, SourceError> {
    let Some((&tag, mut data)) = payload.split_first() else {
        return Err(SourceError::PollDeltaFailed {
            reason: "empty pgoutput message".to_string(),
        });
    };
    if tag != b'C' {
        return Err(SourceError::PollDeltaFailed {
            reason: "expected pgoutput COMMIT frame".to_string(),
        });
    }
    take_byte(&mut data)?;
    take_bytes(&mut data, 8)?;
    let end_lsn = u64::from_be_bytes(take_bytes(&mut data, 8)?.try_into().map_err(|_| {
        SourceError::PollDeltaFailed {
            reason: "truncated pgoutput message".to_string(),
        }
    })?);
    take_bytes(&mut data, 8)?;
    if !data.is_empty() {
        return Err(SourceError::PollDeltaFailed {
            reason: "malformed pgoutput COMMIT frame".to_string(),
        });
    }
    Ok(PgLsn(end_lsn))
}

fn take_i32(data: &mut &[u8]) -> Result<i32, SourceError> {
    Ok(i32::from_be_bytes(
        take_bytes(data, 4)?
            .try_into()
            .map_err(|_| SourceError::PollDeltaFailed {
                reason: "truncated pgoutput message".to_string(),
            })?,
    ))
}

fn take_cstring<'a>(data: &mut &'a [u8]) -> Result<&'a [u8], SourceError> {
    let length =
        data.iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| SourceError::PollDeltaFailed {
                reason: "unterminated pgoutput string".to_string(),
            })?;
    let value = take_bytes(data, length)?;
    take_byte(data)?;
    Ok(value)
}

fn decode_pgoutput(payload: &[u8]) -> Result<CdcChange, SourceError> {
    let text = std::str::from_utf8(payload).map_err(|_| SourceError::PollDeltaFailed {
        reason: "pgoutput payload is not UTF-8 in the supported test decoder".to_string(),
    })?;
    let fields: Vec<&str> = text.split('|').collect();
    if fields.len() < 6 || fields[0] != "B" {
        return Err(SourceError::PollDeltaFailed {
            reason: "invalid pgoutput change; expected B|lsn|table_id|I|U|D|key|values".to_string(),
        });
    }
    let operation = parse_operation(fields[3])?;
    let old_values = match operation {
        CdcOperation::Update | CdcOperation::Delete => Some(parse_values(fields[5])?),
        CdcOperation::Insert => None,
    };
    let new_values = match operation {
        CdcOperation::Insert => Some(parse_values(fields[5])?),
        CdcOperation::Update => Some(parse_values(fields.get(6).ok_or_else(|| {
            SourceError::PollDeltaFailed {
                reason: "pgoutput UPDATE requires old and new values".to_string(),
            }
        })?)?),
        CdcOperation::Delete => None,
    };
    make_change(
        PgLsn::parse(fields[1])?,
        fields[2],
        operation,
        fields[4].as_bytes(),
        old_values,
        new_values,
    )
}

fn decode_wal2json(payload: &[u8]) -> Result<CdcChange, SourceError> {
    let decoded: Wal2JsonChange =
        serde_json::from_slice(payload).map_err(|error| SourceError::PollDeltaFailed {
            reason: format!("invalid wal2json change: {error}"),
        })?;
    make_change(
        PgLsn::parse(&decoded.lsn)?,
        &decoded.table_id.to_string(),
        parse_operation(&decoded.op)?,
        decoded.key.as_bytes(),
        decoded.old,
        decoded.new,
    )
}

fn make_change(
    lsn: PgLsn,
    table_id: &str,
    operation: CdcOperation,
    key: &[u8],
    old_values: Option<Vec<i64>>,
    new_values: Option<Vec<i64>>,
) -> Result<CdcChange, SourceError> {
    let table_id = table_id.parse().map_err(|_| SourceError::PollDeltaFailed {
        reason: format!("invalid PostgreSQL table id '{table_id}'"),
    })?;
    Ok(CdcChange {
        lsn,
        table_id,
        primary_key: key.to_vec(),
        row_id: CdcChange::row_id_for(table_id, key),
        operation,
        old_values,
        new_values,
    })
}

fn parse_operation(value: &str) -> Result<CdcOperation, SourceError> {
    match value.to_ascii_lowercase().as_str() {
        "i" | "insert" => Ok(CdcOperation::Insert),
        "u" | "update" => Ok(CdcOperation::Update),
        "d" | "delete" => Ok(CdcOperation::Delete),
        _ => Err(SourceError::PollDeltaFailed {
            reason: format!("unsupported CDC operation '{value}'"),
        }),
    }
}

fn parse_values(value: &str) -> Result<Vec<i64>, SourceError> {
    value
        .split(',')
        .map(|value| {
            value.parse().map_err(|_| SourceError::PollDeltaFailed {
                reason: format!("CDC value '{value}' is not an i64"),
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_types::ids::ConnectorId;

    fn test_source(format: CdcWireFormat) -> PostgresCdcSource {
        PostgresCdcSource::new(
            ConnectorId(9999),
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            format,
        )
    }

    #[test]
    fn native_pgoutput_relation_insert_and_update_preserve_exact_images() {
        let relation_id = 42_u32;
        let mut relation = vec![b'R'];
        relation.extend_from_slice(&relation_id.to_be_bytes());
        relation.extend_from_slice(b"public\0orders\0d");
        relation.extend_from_slice(&1_u16.to_be_bytes());
        relation.extend_from_slice(&[0]);
        relation.extend_from_slice(b"id\0");
        relation.extend_from_slice(&20_u32.to_be_bytes());
        relation.extend_from_slice(&(-1_i32).to_be_bytes());
        let mut relations = std::collections::HashMap::new();
        assert_eq!(
            decode_native_pgoutput_message(&mut relations, 1, PgLsn(9), &relation).unwrap(),
            None
        );

        let tuple = |value: i64| {
            let text = value.to_string();
            let mut bytes = Vec::from(*b"N");
            bytes.extend_from_slice(&1_u16.to_be_bytes());
            bytes.push(b't');
            bytes.extend_from_slice(&(text.len() as i32).to_be_bytes());
            bytes.extend_from_slice(text.as_bytes());
            bytes
        };
        let mut insert = vec![b'I'];
        insert.extend_from_slice(&relation_id.to_be_bytes());
        insert.extend(tuple(7));
        assert_eq!(
            decode_native_pgoutput_message(&mut relations, 1, PgLsn(10), &insert).unwrap(),
            Some(CdcChange {
                lsn: PgLsn(10),
                table_id: relation_id,
                primary_key: 7_i64.to_be_bytes().to_vec(),
                row_id: CdcChange::row_id_for(relation_id, &7_i64.to_be_bytes()),
                operation: CdcOperation::Insert,
                old_values: None,
                new_values: Some(vec![7]),
            })
        );

        let mut update = vec![b'U'];
        update.extend_from_slice(&relation_id.to_be_bytes());
        let mut old = tuple(7);
        old[0] = b'O';
        update.extend(old);
        update.extend(tuple(8));
        assert_eq!(
            decode_native_pgoutput_message(&mut relations, 1, PgLsn(11), &update).unwrap(),
            Some(CdcChange {
                lsn: PgLsn(11),
                table_id: relation_id,
                primary_key: 7_i64.to_be_bytes().to_vec(),
                row_id: CdcChange::row_id_for(relation_id, &7_i64.to_be_bytes()),
                operation: CdcOperation::Update,
                old_values: Some(vec![7]),
                new_values: Some(vec![8]),
            })
        );
    }

    #[test]
    fn relation_aware_event_decoder_preserves_exact_xid_route_tuple_and_commit_lsn() {
        let mut xid = None;
        let mut begin = vec![b'B'];
        begin.extend_from_slice(&100_u64.to_be_bytes());
        begin.extend_from_slice(&0_u64.to_be_bytes());
        begin.extend_from_slice(&52_u32.to_be_bytes());
        assert_eq!(
            decode_pgoutput_event(&mut xid, &begin).unwrap(),
            PgOutputEvent::Begin { xid: 52 }
        );

        let mut relation = vec![b'R'];
        relation.extend_from_slice(&7_u32.to_be_bytes());
        relation.extend_from_slice(b"public\0orders\0f");
        relation.extend_from_slice(&2_u16.to_be_bytes());
        relation.extend_from_slice(&[1]);
        relation.extend_from_slice(b"id\0");
        relation.extend_from_slice(&20_u32.to_be_bytes());
        relation.extend_from_slice(&(-1_i32).to_be_bytes());
        relation.extend_from_slice(&[0]);
        relation.extend_from_slice(b"note\0");
        relation.extend_from_slice(&25_u32.to_be_bytes());
        relation.extend_from_slice(&(-1_i32).to_be_bytes());
        assert_eq!(
            decode_pgoutput_event(&mut xid, &relation).unwrap(),
            PgOutputEvent::Relation {
                xid: 52,
                relation: PgOutputRelationMetadata {
                    relation_id: 7,
                    namespace: "public".to_string(),
                    name: "orders".to_string(),
                    replica_identity: b'f',
                    columns: vec![
                        PgOutputColumn {
                            flags: 1,
                            name: "id".to_string(),
                            type_oid: 20,
                            type_modifier: -1,
                        },
                        PgOutputColumn {
                            flags: 0,
                            name: "note".to_string(),
                            type_oid: 25,
                            type_modifier: -1,
                        },
                    ],
                },
            }
        );

        let mut insert = vec![b'I'];
        insert.extend_from_slice(&7_u32.to_be_bytes());
        insert.push(b'N');
        insert.extend_from_slice(&2_u16.to_be_bytes());
        insert.push(b't');
        insert.extend_from_slice(&1_i32.to_be_bytes());
        insert.push(b'9');
        insert.push(b'n');
        assert_eq!(
            decode_pgoutput_event(&mut xid, &insert).unwrap(),
            PgOutputEvent::Insert {
                xid: 52,
                relation_id: 7,
                new_values: vec![Some("9".to_string()), None],
            }
        );

        let mut commit = vec![b'C', 0];
        commit.extend_from_slice(&110_u64.to_be_bytes());
        commit.extend_from_slice(&120_u64.to_be_bytes());
        commit.extend_from_slice(&0_u64.to_be_bytes());
        assert_eq!(
            decode_pgoutput_event(&mut xid, &commit).unwrap(),
            PgOutputEvent::Commit {
                xid: 52,
                commit_lsn: PgLsn(120),
            }
        );
        assert_eq!(xid, None);
    }

    #[test]
    fn cdc_pgoutput_truncated_tuple_returns_rs_error_no_panic() {
        let payload = b"B|0/16B3748|1001|I";
        let res = decode_pgoutput(payload);
        assert!(res.is_err());
    }

    #[test]
    fn cdc_wal2json_malformed_json_returns_rs_error_no_panic() {
        let payload = b"{\"change\": [invalid json";
        let res = decode_wal2json(payload);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn cdc_fence_captures_committed_snapshot_and_queued_live_lsn() {
        let mut source = test_source(CdcWireFormat::PgOutput);
        source
            .commit_offset(4, PgLsn(10).to_offset_token())
            .await
            .unwrap();
        source
            .enqueue(
                CdcChange {
                    lsn: PgLsn(20),
                    table_id: 1,
                    primary_key: b"key".to_vec(),
                    row_id: 1,
                    operation: CdcOperation::Insert,
                    old_values: None,
                    new_values: Some(vec![1]),
                },
                1,
            )
            .unwrap();
        assert_eq!(
            source.capture_snapshot_delta_fence(None).await.unwrap(),
            SnapshotDeltaFence::new(PgLsn(10).to_offset_token(), PgLsn(20).to_offset_token())
        );
    }
}
