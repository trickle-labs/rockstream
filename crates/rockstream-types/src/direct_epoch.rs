//! Bounded capabilities for actor-authorized direct source epochs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::data_plane::{SourceDeltaRequest, DEPLOYMENT_DESCRIPTOR_VERSION};
use crate::ids::{LeaseToken, ShardId, WorkloadId};

pub const DIRECT_EPOCH_GRANT_TTL_MS: u64 = 60_000;
pub const MAX_DIRECT_EPOCH_GRANTS: usize = 4096;

/// Only this metadata, not source rows, crosses the control authorization path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceEpochMetadata {
    pub version: u32,
    pub workload_id: WorkloadId,
    pub request_id: String,
    pub source: String,
    pub epoch: u64,
    pub input_digest: [u8; 32],
}

impl SourceEpochMetadata {
    pub fn from_request(request: &SourceDeltaRequest) -> Result<Self, serde_json::Error> {
        Ok(Self {
            version: request.version,
            workload_id: request.workload_id,
            request_id: request.request_id.clone(),
            source: request.source.clone(),
            epoch: request.epoch,
            input_digest: Sha256::digest(serde_json::to_vec(request)?).into(),
        })
    }

    pub fn is_valid(&self) -> bool {
        self.version == DEPLOYMENT_DESCRIPTOR_VERSION
            && self.epoch > 0
            && !self.request_id.is_empty()
            && self.request_id.len() <= 256
            && !self.source.is_empty()
            && self.source.len() <= 256
    }
}

/// Installed over the authenticated control channel before the gateway sees
/// the opaque capability. Ownership changes invalidate its member fences.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectEpochGrant {
    pub grant_id: uuid::Uuid,
    pub metadata: SourceEpochMetadata,
    pub expires_at_ms: u64,
    #[serde(with = "member_wire")]
    pub members: BTreeMap<ShardId, LeaseToken>,
}

// Internally tagged control messages buffer their contents in Serde. JSON
// object-key coercion is lost for integer newtypes in that buffer, so members
// use explicit tuples on the wire rather than stringified object keys.
mod member_wire {
    use super::*;

    pub fn serialize<S: serde::Serializer>(
        members: &BTreeMap<ShardId, LeaseToken>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        members.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<ShardId, LeaseToken>, D::Error> {
        let entries = Vec::<(ShardId, LeaseToken)>::deserialize(deserializer)?;
        if entries.len() > 4096 {
            return Err(serde::de::Error::custom(
                "direct grant membership exceeds limit",
            ));
        }
        let count = entries.len();
        let members: BTreeMap<_, _> = entries.into_iter().collect();
        if members.len() != count {
            return Err(serde::de::Error::custom("duplicate direct grant member"));
        }
        Ok(members)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizedOwnedContext {
    pub direct_grant_id: uuid::Uuid,
    pub metadata: SourceEpochMetadata,
    pub inner: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_plane::RuntimeRow;

    #[test]
    fn grant_members_roundtrip_through_tagged_control_messages_without_key_coercion() {
        let grant = DirectEpochGrant {
            grant_id: uuid::Uuid::from_u128(7),
            metadata: SourceEpochMetadata {
                version: DEPLOYMENT_DESCRIPTOR_VERSION,
                workload_id: WorkloadId(4),
                request_id: "epoch-1".into(),
                source: "events".into(),
                epoch: 1,
                input_digest: [3; 32],
            },
            expires_at_ms: 1000,
            members: BTreeMap::from([(ShardId(1), LeaseToken(7)), (ShardId(2), LeaseToken(9))]),
        };
        let message = crate::topology::ControlMessage::InstallDirectEpochGrant {
            grant: grant.clone(),
        };
        let mut wire = serde_json::to_value(&message).unwrap();
        assert_eq!(
            wire["grant"]["members"],
            serde_json::json!([[1, 7], [2, 9]])
        );
        let actual: crate::topology::ControlMessage =
            serde_json::from_str(&serde_json::to_string(&message).unwrap()).unwrap();
        let crate::topology::ControlMessage::InstallDirectEpochGrant { grant: actual } = actual
        else {
            panic!("grant message changed type")
        };
        assert_eq!(actual, grant);
        wire["grant"]["members"] = serde_json::json!([[1, 7], [1, 9]]);
        assert!(
            serde_json::from_value::<crate::topology::ControlMessage>(wire)
                .unwrap_err()
                .to_string()
                .contains("duplicate direct grant member")
        );
    }

    #[test]
    fn authorization_metadata_binds_every_row_and_epoch_without_carrying_rows() {
        let request = SourceDeltaRequest {
            version: DEPLOYMENT_DESCRIPTOR_VERSION,
            request_id: "direct-1".into(),
            workload_id: WorkloadId(4),
            epoch: 9,
            source: "events".into(),
            rows: vec![RuntimeRow {
                values_tsv: "1\t2".into(),
                weight: -1,
            }],
        };
        let metadata = SourceEpochMetadata::from_request(&request).unwrap();
        assert!(metadata.is_valid());
        assert_eq!(
            metadata.input_digest,
            <[u8; 32]>::from(Sha256::digest(serde_json::to_vec(&request).unwrap()))
        );
        let mut changed = request.clone();
        changed.rows[0].weight = 1;
        assert_ne!(
            SourceEpochMetadata::from_request(&changed).unwrap(),
            metadata
        );
        changed = request;
        changed.epoch += 1;
        assert_ne!(
            SourceEpochMetadata::from_request(&changed).unwrap(),
            metadata
        );
        assert!(serde_json::to_value(metadata)
            .unwrap()
            .get("rows")
            .is_none());
    }
}
