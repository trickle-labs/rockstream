//! Source-scoped PostgreSQL pgoutput ownership and transaction buffering.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;

use rockstream_connectors::{
    CdcOperation, PgLsn, PostgresCdcSource, SourceOwnerLease, SourceRuntimeCoordinator,
    POSTGRES_CDC_MAX_TRANSACTION_BYTES, POSTGRES_CDC_TRANSACTION_MEMORY_BYTES,
};
use rockstream_ops::spill::{SerdeSpill, SpillKey, SpillableArrangement};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::ConnectorId;
use serde::{Deserialize, Serialize};

use crate::GatewayError;

const IDENTITY_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentityV1 {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub slot: String,
    pub publication: String,
    pub auth_principal: String,
    pub credential_ref: String,
}

pub type SourceIdentity = SourceIdentityV1;

impl SourceIdentityV1 {
    // Source identity is a complete durable key, so construction takes every key dimension.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: impl Into<String>,
        port: Option<u16>,
        database: impl Into<String>,
        slot: impl Into<String>,
        publication: impl Into<String>,
        auth_principal: impl Into<String>,
        credential_ref: impl Into<String>,
    ) -> Result<Self, GatewayError> {
        let host = canonical_host(&host.into())?;
        let identity = Self {
            host,
            port: port.unwrap_or(5432),
            database: database.into(),
            slot: slot.into(),
            publication: publication.into(),
            auth_principal: auth_principal.into(),
            credential_ref: credential_ref.into(),
        };
        if [
            identity.database.as_str(),
            identity.slot.as_str(),
            identity.publication.as_str(),
            identity.auth_principal.as_str(),
            identity.credential_ref.as_str(),
        ]
        .iter()
        .any(|field| field.is_empty())
        {
            return Err(coordinator_error(
                "RS-4013: pgoutput source identity fields may not be empty",
            ));
        }
        Ok(identity)
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_field(&mut bytes, &IDENTITY_VERSION.to_be_bytes());
        push_field(&mut bytes, self.host.as_bytes());
        push_field(&mut bytes, &self.port.to_be_bytes());
        push_field(&mut bytes, self.database.as_bytes());
        push_field(&mut bytes, self.slot.as_bytes());
        push_field(&mut bytes, self.publication.as_bytes());
        push_field(&mut bytes, self.auth_principal.as_bytes());
        push_field(&mut bytes, self.credential_ref.as_bytes());
        bytes
    }

    pub fn connector_id(&self) -> ConnectorId {
        ConnectorId(rockstream_types::rendezvous::fnv1a_64(
            &self.canonical_bytes(),
        ))
    }

    pub fn has_same_physical_slot(&self, other: &Self) -> bool {
        self.host == other.host
            && self.port == other.port
            && self.database == other.database
            && self.slot == other.slot
    }

    fn physical_slot_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_field(&mut bytes, self.host.as_bytes());
        push_field(&mut bytes, &self.port.to_be_bytes());
        push_field(&mut bytes, self.database.as_bytes());
        push_field(&mut bytes, self.slot.as_bytes());
        bytes
    }

    pub async fn register(&self, db: &ShardDb) -> Result<(), GatewayError> {
        let connector_id = self.connector_id();
        let identity_key = identity_key(connector_id);
        if let Some(stored) = db.get(&identity_key).await? {
            let stored: Self = serde_json::from_slice(&stored)
                .map_err(|error| coordinator_error(&format!("decode source identity: {error}")))?;
            if stored != *self {
                return Err(coordinator_error(
                    "RS-4013: source identity hash collision has a different stored preimage",
                ));
            }
        }

        let physical = PhysicalSlotBinding {
            version: IDENTITY_VERSION,
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            slot: self.slot.clone(),
            connector_id,
        };
        let slot_key = physical_slot_key(self);
        if let Some(stored) = db.get(&slot_key).await? {
            let stored: PhysicalSlotBinding = serde_json::from_slice(&stored)
                .map_err(|error| coordinator_error(&format!("decode slot binding: {error}")))?;
            if stored != physical {
                return Err(coordinator_error(&format!(
                    "RS-4013: physical pgoutput slot is already owned by source {}",
                    stored.connector_id
                )));
            }
        }

        let mut batch = WriteBatch::new();
        batch.put(
            &identity_key,
            &serde_json::to_vec(self)
                .map_err(|error| coordinator_error(&format!("encode source identity: {error}")))?,
        );
        batch.put(
            &slot_key,
            &serde_json::to_vec(&physical)
                .map_err(|error| coordinator_error(&format!("encode slot binding: {error}")))?,
        );
        db.write_batch(batch).await?;
        db.flush().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalSlotBinding {
    version: u16,
    host: String,
    port: u16,
    database: String,
    slot: String,
    connector_id: ConnectorId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl ReplicaIdentity {
    pub fn from_wire(value: u8) -> Result<Self, GatewayError> {
        match value {
            b'd' => Ok(Self::Default),
            b'n' => Ok(Self::Nothing),
            b'f' => Ok(Self::Full),
            b'i' => Ok(Self::Index),
            _ => Err(coordinator_error("RS-1002: unknown replica identity")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnRoute {
    pub upstream_name: String,
    pub imported_name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub nullable: bool,
    pub has_default: bool,
    pub key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationRoute {
    pub version: u16,
    pub relation_id: u32,
    pub upstream_namespace: String,
    pub upstream_relation: String,
    pub imported_table_id: u64,
    pub imported_table_name: String,
    pub columns: Vec<ColumnRoute>,
    pub replica_identity: ReplicaIdentity,
    pub schema_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationChange {
    Unchanged,
    Compatible,
    Breaking(String),
}

impl RelationRoute {
    pub fn classify(&self, next: &Self) -> RelationChange {
        if self.relation_id != next.relation_id
            || self.upstream_namespace != next.upstream_namespace
            || self.upstream_relation != next.upstream_relation
            || self.imported_table_id != next.imported_table_id
            || self.imported_table_name != next.imported_table_name
            || self.replica_identity != next.replica_identity
        {
            return RelationChange::Breaking("relation identity changed".to_string());
        }
        if self.columns == next.columns {
            return RelationChange::Unchanged;
        }
        if next.columns.len() < self.columns.len() {
            return RelationChange::Breaking("column was dropped".to_string());
        }
        for (old, new) in self.columns.iter().zip(&next.columns) {
            if old.upstream_name != new.upstream_name
                || old.imported_name != new.imported_name
                || old.key != new.key
            {
                return RelationChange::Breaking(
                    "column was renamed, reordered, or changed key identity".to_string(),
                );
            }
            if !lossless_type_change(old, new) {
                return RelationChange::Breaking("column type narrowed or changed".to_string());
            }
        }
        if next.columns[self.columns.len()..]
            .iter()
            .any(|column| !column.nullable && !column.has_default)
        {
            return RelationChange::Breaking(
                "new column is neither nullable nor defaulted".to_string(),
            );
        }
        RelationChange::Compatible
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EnvelopeKey {
    pub xid: u32,
    pub sequence: u64,
}

impl SpillKey for EnvelopeKey {
    fn to_spill_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&self.xid.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes
    }

    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, rockstream_ops::OpError> {
        if bytes.len() != 12 {
            return Err(rockstream_ops::OpError::storage_error(
                "invalid pgoutput envelope key length".to_string(),
            ));
        }
        let xid = u32::from_be_bytes(bytes[..4].try_into().map_err(|_| {
            rockstream_ops::OpError::storage_error("invalid pgoutput xid".to_string())
        })?);
        let sequence = u64::from_be_bytes(bytes[4..].try_into().map_err(|_| {
            rockstream_ops::OpError::storage_error("invalid pgoutput sequence".to_string())
        })?);
        Ok(Self { xid, sequence })
    }

    fn byte_size(&self) -> usize {
        12
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedChange {
    pub relation_id: u32,
    pub operation: CdcOperation,
    pub old_values: Option<Vec<Option<String>>>,
    pub new_values: Option<Vec<Option<String>>>,
    pub schema_version: u64,
}

#[derive(Debug, Clone)]
pub struct BufferedPgOutputEnvelope {
    pub xid: u32,
    pub commit_lsn: PgLsn,
    pub changes: Vec<EncodedChange>,
    pub route_updates: Vec<RelationRoute>,
}

#[derive(Debug)]
pub struct ActiveEnvelope {
    pub xid: u32,
    pub total_encoded_bytes: usize,
    next_sequence: u64,
    route_updates: BTreeMap<u32, RelationRoute>,
    unrouted_relations: BTreeSet<u32>,
}

pub struct SharedPgOutputCoordinator {
    pub source_identity: SourceIdentity,
    pub connector_id: ConnectorId,
    pub runtime: SourceRuntimeCoordinator<PostgresCdcSource>,
    pub relation_routes: BTreeMap<u32, RelationRoute>,
    pub blocked_state: Option<BlockedRelationState>,
    shard_db: Arc<ShardDb>,
    envelope_buffer: SpillableArrangement<EnvelopeKey, SerdeSpill<EncodedChange>>,
    pub active_envelope: Option<ActiveEnvelope>,
    pub owner_lease: Option<SourceOwnerLease>,
    pub attached_view_count: usize,
    pub affected_view_count: usize,
    aliases: BTreeSet<String>,
    activating_views: BTreeSet<String>,
}

impl SharedPgOutputCoordinator {
    pub fn new(
        source_identity: SourceIdentity,
        runtime: SourceRuntimeCoordinator<PostgresCdcSource>,
        shard_db: Arc<ShardDb>,
    ) -> Self {
        let connector_id = source_identity.connector_id();
        Self {
            source_identity,
            connector_id,
            runtime,
            relation_routes: BTreeMap::new(),
            blocked_state: None,
            shard_db: Arc::clone(&shard_db),
            envelope_buffer: SpillableArrangement::new(
                Some(shard_db),
                spill_prefix(connector_id),
                POSTGRES_CDC_TRANSACTION_MEMORY_BYTES,
            ),
            active_envelope: None,
            owner_lease: None,
            attached_view_count: 0,
            affected_view_count: 0,
            aliases: BTreeSet::new(),
            activating_views: BTreeSet::new(),
        }
    }

    pub async fn commit_envelope(
        &mut self,
        envelope: BufferedPgOutputEnvelope,
        gateway: &crate::server::GatewayHandler,
        shard_db: &Arc<ShardDb>,
    ) -> Result<(), GatewayError> {
        gateway
            .commit_pgoutput_envelope(self, envelope, shard_db)
            .await
    }

    pub fn attach_alias(&mut self, source_name: impl Into<String>) {
        self.aliases.insert(source_name.into());
    }

    pub fn shares_shard(&self, shard_db: &Arc<ShardDb>) -> bool {
        Arc::ptr_eq(&self.shard_db, shard_db)
    }

    pub fn aliases(&self) -> impl Iterator<Item = &str> {
        self.aliases.iter().map(String::as_str)
    }

    pub fn activate_view(&mut self, view_name: impl Into<String>) {
        self.activating_views.insert(view_name.into());
    }

    pub fn activating_views(&self) -> impl Iterator<Item = &str> {
        self.activating_views.iter().map(String::as_str)
    }

    pub async fn restore_catalog(&mut self, db: &ShardDb) -> Result<(), GatewayError> {
        self.source_identity.register(db).await?;
        self.relation_routes.clear();
        for (_, value) in db.scan_prefix(&relation_prefix(self.connector_id)).await? {
            let route: RelationRoute = serde_json::from_slice(&value)
                .map_err(|error| coordinator_error(&format!("decode relation route: {error}")))?;
            self.relation_routes.insert(route.relation_id, route);
        }
        self.blocked_state = db
            .get(&blocked_key(self.connector_id))
            .await?
            .map(|value| {
                serde_json::from_slice(&value).map_err(|error| {
                    coordinator_error(&format!("decode blocked relation state: {error}"))
                })
            })
            .transpose()?;
        Ok(())
    }

    pub fn begin(&mut self, xid: u32) -> Result<(), GatewayError> {
        if self.active_envelope.is_some() {
            return Err(protocol_error("nested BEGIN"));
        }
        self.active_envelope = Some(ActiveEnvelope {
            xid,
            total_encoded_bytes: 0,
            next_sequence: 0,
            route_updates: BTreeMap::new(),
            unrouted_relations: BTreeSet::new(),
        });
        self.affected_view_count = 0;
        Ok(())
    }

    pub fn stage_route(&mut self, xid: u32, route: RelationRoute) -> Result<(), GatewayError> {
        let active = self.require_xid(xid)?;
        active.unrouted_relations.remove(&route.relation_id);
        active.route_updates.insert(route.relation_id, route);
        Ok(())
    }

    pub fn stage_unrouted(&mut self, xid: u32, relation_id: u32) -> Result<(), GatewayError> {
        self.require_xid(xid)?
            .unrouted_relations
            .insert(relation_id);
        Ok(())
    }

    pub fn next_schema_version(&self) -> Result<u64, GatewayError> {
        self.relation_routes
            .values()
            .chain(
                self.active_envelope
                    .iter()
                    .flat_map(|active| active.route_updates.values()),
            )
            .map(|route| route.schema_version)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| coordinator_error("RS-1002: pgoutput schema version exhausted"))
    }

    pub fn push_change(
        &mut self,
        xid: u32,
        relation_id: u32,
        operation: CdcOperation,
        old_values: Option<Vec<Option<String>>>,
        new_values: Option<Vec<Option<String>>>,
    ) -> Result<(), GatewayError> {
        self.require_xid(xid)?;
        let schema_version = self
            .active_envelope
            .as_ref()
            .and_then(|active| active.route_updates.get(&relation_id))
            .or_else(|| self.relation_routes.get(&relation_id))
            .map(|route| route.schema_version);
        let Some(schema_version) = schema_version else {
            if self
                .active_envelope
                .as_ref()
                .is_some_and(|active| active.unrouted_relations.contains(&relation_id))
            {
                return Ok(());
            }
            return Err(protocol_error(&format!(
                "row for relation {relation_id} has no durable route or preceding Relation message"
            )));
        };
        let change = EncodedChange {
            relation_id,
            operation,
            old_values,
            new_values,
            schema_version,
        };
        let encoded_bytes = serde_json::to_vec(&change)
            .map_err(|error| coordinator_error(&format!("encode pgoutput change: {error}")))?
            .len();
        let active = self.require_xid(xid)?;
        let next_total = active.total_encoded_bytes.saturating_add(encoded_bytes);
        if next_total > POSTGRES_CDC_MAX_TRANSACTION_BYTES {
            return Err(coordinator_error(
                "RS-4014: pgoutput transaction exceeds POSTGRES_CDC_MAX_TRANSACTION_BYTES; replication is paused",
            ));
        }
        let key = EnvelopeKey {
            xid,
            sequence: active.next_sequence,
        };
        active.next_sequence = active.next_sequence.saturating_add(1);
        active.total_encoded_bytes = next_total;
        self.envelope_buffer
            .insert(key, SerdeSpill(change))
            .map_err(|error| coordinator_error(&format!("spill pgoutput change: {error}")))?;
        Ok(())
    }

    pub fn finish_envelope(
        &mut self,
        xid: u32,
        commit_lsn: PgLsn,
    ) -> Result<BufferedPgOutputEnvelope, GatewayError> {
        let active = match self.active_envelope.as_ref() {
            Some(active) if active.xid == xid => active,
            Some(active) => {
                return Err(protocol_error(&format!(
                    "xid mismatch: active {}, received {xid}",
                    active.xid
                )))
            }
            None => return Err(protocol_error("COMMIT without BEGIN")),
        };
        if PgLsn::from_offset_token(self.runtime.committed_offset())
            .map_err(|error| coordinator_error(&error.to_string()))?
            >= commit_lsn
        {
            return Err(protocol_error(
                "commit LSN is not above the source checkpoint",
            ));
        }
        let route_updates = active.route_updates.values().cloned().collect();
        let mut entries = self
            .envelope_buffer
            .scan_all()
            .map_err(|error| coordinator_error(&format!("scan pgoutput spill: {error}")))?;
        entries.sort_by_key(|entry| entry.0.to_spill_bytes());
        let changes = entries
            .into_iter()
            .filter(|(key, _)| key.xid == xid)
            .map(|(_, value)| value.0)
            .collect();
        Ok(BufferedPgOutputEnvelope {
            xid,
            commit_lsn,
            changes,
            route_updates,
        })
    }

    pub async fn cleanup_committed(&mut self, db: &ShardDb) -> Result<(), GatewayError> {
        let active = self.active_envelope.as_ref().ok_or_else(|| {
            protocol_error("spill cleanup requested without an active transaction")
        })?;
        let xid = active.xid;
        let route_updates = active.route_updates.clone();
        let keys = self
            .envelope_buffer
            .scan_all()
            .map_err(|error| coordinator_error(&format!("scan pgoutput spill: {error}")))?
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| key.xid == xid)
            .collect::<Vec<_>>();
        for key in keys {
            self.envelope_buffer
                .remove(&key)
                .map_err(|error| coordinator_error(&format!("delete pgoutput spill: {error}")))?;
        }
        db.flush().await?;
        self.relation_routes.extend(route_updates);
        self.active_envelope = None;
        self.activating_views.clear();
        Ok(())
    }

    pub async fn cleanup_recovered_spill(&mut self, db: &ShardDb) -> Result<(), GatewayError> {
        self.envelope_buffer
            .populate_spilled_keys_from_db()
            .map_err(|error| coordinator_error(&format!("restore pgoutput spill keys: {error}")))?;
        let keys = self
            .envelope_buffer
            .scan_all()
            .map_err(|error| coordinator_error(&format!("scan recovered pgoutput spill: {error}")))?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        for key in keys {
            self.envelope_buffer
                .remove(&key)
                .map_err(|error| coordinator_error(&format!("delete recovered spill: {error}")))?;
        }
        db.flush().await?;
        Ok(())
    }

    pub async fn drop_durable_state(&mut self, db: &ShardDb) -> Result<(), GatewayError> {
        self.runtime
            .drop_source()
            .await
            .map_err(|error| coordinator_error(&error.to_string()))?;
        self.cleanup_recovered_spill(db).await?;
        loop {
            let (records, _) = db
                .scan_prefix_bounded(&coordinator_prefix(self.connector_id), 1024 * 1024)
                .await?;
            if records.is_empty() {
                break;
            }
            let mut batch = WriteBatch::new();
            for (key, _) in records {
                batch.delete(&key);
            }
            db.write_batch(batch).await?;
            db.flush().await?;
        }
        db.delete(&physical_slot_key(&self.source_identity)).await?;
        db.flush().await?;
        Ok(())
    }

    pub fn append_route_updates(
        &self,
        batch: &mut WriteBatch,
        routes: &[RelationRoute],
    ) -> Result<(), GatewayError> {
        for route in routes {
            let encoded = serde_json::to_vec(route)
                .map_err(|error| coordinator_error(&format!("encode relation route: {error}")))?;
            batch.put(
                &relation_key(self.connector_id, route.relation_id),
                &encoded,
            );
            batch.put(
                &schema_history_key(self.connector_id, route.schema_version),
                &encoded,
            );
        }
        Ok(())
    }

    pub fn envelope_bytes(&self) -> usize {
        self.active_envelope
            .as_ref()
            .map_or(0, |active| active.total_encoded_bytes)
    }

    pub fn in_memory_bytes(&self) -> u64 {
        self.envelope_buffer.state_bytes()
    }

    pub fn spill_bytes(&self) -> u64 {
        self.envelope_buffer.spilled_bytes()
    }

    fn require_xid(&mut self, xid: u32) -> Result<&mut ActiveEnvelope, GatewayError> {
        match self.active_envelope.as_mut() {
            Some(active) if active.xid == xid => Ok(active),
            Some(active) => Err(protocol_error(&format!(
                "xid mismatch: active {}, received {xid}",
                active.xid
            ))),
            None => Err(protocol_error("row, relation, or COMMIT without BEGIN")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedRelationState {
    pub code: String,
    pub xid: u32,
    pub relation: rockstream_connectors::PgOutputRelationMetadata,
    pub last_safe_lsn: PgLsn,
}

pub fn append_blocked_state(
    batch: &mut WriteBatch,
    connector_id: ConnectorId,
    blocked: &BlockedRelationState,
) -> Result<(), GatewayError> {
    batch.put(
        &blocked_key(connector_id),
        &serde_json::to_vec(blocked)
            .map_err(|error| coordinator_error(&format!("encode blocked state: {error}")))?,
    );
    Ok(())
}

fn canonical_host(host: &str) -> Result<String, GatewayError> {
    let host = host.trim();
    if host.is_empty() {
        return Err(coordinator_error("RS-4013: pgoutput host may not be empty"));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ip.to_string());
    }
    let lower = host.to_ascii_lowercase();
    Ok(lower.strip_suffix('.').unwrap_or(&lower).to_string())
}

fn push_field(output: &mut Vec<u8>, field: &[u8]) {
    output.extend_from_slice(&(field.len() as u32).to_be_bytes());
    output.extend_from_slice(field);
}

fn lossless_type_change(old: &ColumnRoute, new: &ColumnRoute) -> bool {
    old.type_oid == new.type_oid
        && (old.type_modifier == new.type_modifier
            || (matches!(old.type_oid, 25 | 1043)
                && (new.type_modifier < 0 || new.type_modifier >= old.type_modifier)))
        || (old.type_oid == 23 && new.type_oid == 20)
}

fn coordinator_prefix(connector_id: ConnectorId) -> Vec<u8> {
    format!("connector/{}/pgoutput/", connector_id.0).into_bytes()
}

fn identity_key(connector_id: ConnectorId) -> Vec<u8> {
    let mut key = coordinator_prefix(connector_id);
    key.extend_from_slice(b"identity/v1");
    key
}

fn relation_prefix(connector_id: ConnectorId) -> Vec<u8> {
    let mut key = coordinator_prefix(connector_id);
    key.extend_from_slice(b"relation/");
    key
}

fn relation_key(connector_id: ConnectorId, relation_id: u32) -> Vec<u8> {
    let mut key = relation_prefix(connector_id);
    key.extend_from_slice(&relation_id.to_be_bytes());
    key
}

fn schema_history_key(connector_id: ConnectorId, schema_version: u64) -> Vec<u8> {
    let mut key = coordinator_prefix(connector_id);
    key.extend_from_slice(b"schema_history/");
    key.extend_from_slice(&schema_version.to_be_bytes());
    key
}

fn blocked_key(connector_id: ConnectorId) -> Vec<u8> {
    let mut key = coordinator_prefix(connector_id);
    key.extend_from_slice(b"blocked");
    key
}

fn physical_slot_key(identity: &SourceIdentityV1) -> Vec<u8> {
    let hash = rockstream_types::rendezvous::fnv1a_64(&identity.physical_slot_bytes());
    format!("connector_slot/{hash:016x}").into_bytes()
}

fn spill_prefix(connector_id: ConnectorId) -> Vec<u8> {
    format!("connector/{}/pgoutput/spill/", connector_id.0).into_bytes()
}

fn protocol_error(detail: &str) -> GatewayError {
    coordinator_error(&format!("RS-4013: pgoutput protocol error: {detail}"))
}

fn coordinator_error(detail: &str) -> GatewayError {
    GatewayError::QueryTimeExecutionFailed {
        detail: detail.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use object_store::memory::InMemory;
    use rockstream_connectors::{
        CdcWireFormat, OffsetToken, SourceCheckpointStore, SourceRuntimeCoordinator,
    };

    fn identity(host: &str) -> SourceIdentityV1 {
        SourceIdentityV1::new(
            host,
            None,
            "Db",
            "Slot",
            "Publication",
            "Principal",
            "vault://pg/main",
        )
        .unwrap()
    }

    fn route(columns: Vec<ColumnRoute>) -> RelationRoute {
        RelationRoute {
            version: 1,
            relation_id: 7,
            upstream_namespace: "public".to_string(),
            upstream_relation: "orders".to_string(),
            imported_table_id: 9,
            imported_table_name: "orders".to_string(),
            columns,
            replica_identity: ReplicaIdentity::Full,
            schema_version: 1,
        }
    }

    fn column(name: &str, type_oid: u32) -> ColumnRoute {
        ColumnRoute {
            upstream_name: name.to_string(),
            imported_name: name.to_string(),
            type_oid,
            type_modifier: -1,
            nullable: false,
            has_default: false,
            key: name == "id",
        }
    }

    async fn coordinator() -> SharedPgOutputCoordinator {
        let identity = identity("db.example");
        let connector_id = identity.connector_id();
        let db = Arc::new(
            ShardDb::builder("pgoutput-envelope", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap(),
        );
        let source = PostgresCdcSource::new(
            connector_id,
            Arc::new(Schema::empty()),
            CdcWireFormat::PgOutput,
        );
        let checkpoints =
            SourceCheckpointStore::new(Arc::clone(&db), connector_id.0 as u128, connector_id);
        SharedPgOutputCoordinator::new(
            identity,
            SourceRuntimeCoordinator::new(
                source,
                connector_id,
                OffsetToken::new(Vec::new()),
                checkpoints,
            ),
            db,
        )
    }

    #[test]
    fn source_identity_canonicalization_and_field_sensitivity_are_exact() {
        let canonical = identity(" Example.COM. ");
        let same = identity("example.com");
        let mut changed = Vec::new();
        for mutate in 0..7 {
            let mut value = same.clone();
            match mutate {
                0 => value.host = "other.example".to_string(),
                1 => value.port = 5433,
                2 => value.database.push('x'),
                3 => value.slot.push('x'),
                4 => value.publication.push('x'),
                5 => value.auth_principal.push('x'),
                6 => value.credential_ref.push('x'),
                _ => {}
            }
            changed.push(value.connector_id());
        }

        assert_eq!(canonical, same);
        assert_eq!(canonical.host, "example.com");
        assert_eq!(canonical.port, 5432);
        assert_eq!(canonical.connector_id(), same.connector_id());
        assert_eq!(
            changed
                .iter()
                .map(|id| *id != canonical.connector_id())
                .collect::<Vec<_>>(),
            vec![true, true, true, true, true, true, true]
        );
    }

    #[test]
    fn relation_change_classification_is_exact() {
        let base = route(vec![column("id", 23), column("value", 1043)]);
        let mut widened = base.clone();
        widened.columns[0].type_oid = 20;
        let mut added = widened.clone();
        let mut optional = column("note", 25);
        optional.nullable = true;
        added.columns.push(optional);
        let mut renamed = base.clone();
        renamed.columns[1].upstream_name = "renamed".to_string();

        assert_eq!(base.classify(&base), RelationChange::Unchanged);
        assert_eq!(base.classify(&widened), RelationChange::Compatible);
        assert_eq!(base.classify(&added), RelationChange::Compatible);
        assert_eq!(
            base.classify(&renamed),
            RelationChange::Breaking(
                "column was renamed, reordered, or changed key identity".to_string()
            )
        );
    }

    #[tokio::test]
    async fn rows_require_an_envelope_and_known_or_explicitly_unrouted_relation() {
        let mut coordinator = coordinator().await;
        let no_begin = coordinator
            .push_change(5, 99, CdcOperation::Insert, None, Some(vec![]))
            .unwrap_err();
        coordinator.begin(5).unwrap();
        let unknown = coordinator
            .push_change(5, 99, CdcOperation::Insert, None, Some(vec![]))
            .unwrap_err();
        coordinator.stage_unrouted(5, 99).unwrap();
        coordinator
            .push_change(5, 99, CdcOperation::Insert, None, Some(vec![]))
            .unwrap();

        let detail = |error| match error {
            GatewayError::QueryTimeExecutionFailed { detail } => detail,
            other => panic!("unexpected error: {other}"),
        };
        assert_eq!(
            (detail(no_begin), detail(unknown), coordinator.envelope_bytes()),
            (
                "RS-4013: pgoutput protocol error: row, relation, or COMMIT without BEGIN"
                    .to_string(),
                "RS-4013: pgoutput protocol error: row for relation 99 has no durable route or preceding Relation message"
                    .to_string(),
                0,
            )
        );
    }

    #[tokio::test]
    async fn durable_identity_preimage_and_physical_slot_collisions_fail_exactly() {
        let db = ShardDb::builder("pgoutput-identity", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let first = identity("db.example");
        first.register(&db).await.unwrap();
        let mut same_slot = first.clone();
        same_slot.publication = "other-publication".to_string();
        assert_eq!(
            same_slot.register(&db).await.unwrap_err().to_string(),
            format!(
                "[RS-2026] query.query_time_execution_failed: query-time execution failed: RS-4013: physical pgoutput slot is already owned by source {}. next_steps: Simplify the query, validate referenced table/view schemas, or materialize the query into a view.",
                first.connector_id()
            )
        );

        let collision = identity("collision.example");
        db.put(
            &identity_key(collision.connector_id()),
            &serde_json::to_vec(&first).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            collision.register(&db).await.unwrap_err().to_string(),
            "[RS-2026] query.query_time_execution_failed: query-time execution failed: RS-4013: source identity hash collision has a different stored preimage. next_steps: Simplify the query, validate referenced table/view schemas, or materialize the query into a view."
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cdc_tx_spilled_two_tables_exact_atomic_batch_oracle() {
        let mut coordinator = coordinator().await;
        coordinator
            .relation_routes
            .insert(7, route(vec![column("id", 25)]));
        let mut second = route(vec![column("id", 25)]);
        second.relation_id = 8;
        second.imported_table_name = "payments".to_string();
        coordinator.relation_routes.insert(8, second);
        coordinator.envelope_buffer.set_memory_limit(64);
        let payload = "x".repeat(65);
        coordinator.begin(55).unwrap();
        coordinator
            .push_change(
                55,
                7,
                CdcOperation::Insert,
                None,
                Some(vec![Some(payload.clone())]),
            )
            .unwrap();
        coordinator
            .push_change(
                55,
                8,
                CdcOperation::Insert,
                None,
                Some(vec![Some(payload.clone())]),
            )
            .unwrap();
        let envelope = coordinator.finish_envelope(55, PgLsn(0x55)).unwrap();
        assert_eq!(
            (
                coordinator.spill_bytes() > 0,
                coordinator.envelope_bytes(),
                envelope
                    .changes
                    .iter()
                    .map(|change| (change.relation_id, change.new_values.clone()))
                    .collect::<Vec<_>>(),
            ),
            (
                true,
                serde_json::to_vec(&EncodedChange {
                    relation_id: 7,
                    operation: CdcOperation::Insert,
                    old_values: None,
                    new_values: Some(vec![Some(payload.clone())]),
                    schema_version: 1,
                })
                .unwrap()
                .len()
                    * 2,
                vec![
                    (7, Some(vec![Some(payload.clone())])),
                    (8, Some(vec![Some(payload)])),
                ],
            ),
        );
    }
}
