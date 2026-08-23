//! Runtime-side bucket-map routing for shard migration (v0.46).

use rockstream_storage::ShardDb;
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{MigrationRecord, MigrationState};
use rockstream_types::timestamp::Epoch;
use thiserror::Error;

/// One logical write routed through the bucket map.
#[derive(Debug, Clone)]
pub struct RoutedWrite {
    pub bucket: u64,
    pub bucket_map_version: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// Routing failures visible to writers/readers during migration.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RoutingError {
    #[error(
        "RS-5032: bucket_map_version mismatch for migrating bucket {bucket}: \
         expected {expected}, got {got}; \
         next_steps: refresh the bucket map and retry under the current version"
    )]
    BucketMapVersionMismatch {
        bucket: u64,
        expected: u64,
        got: u64,
    },
}

/// Routes writes for a single migration.
pub struct DualWriteRouter {
    record: MigrationRecord,
}

impl DualWriteRouter {
    pub fn new(record: MigrationRecord) -> Self {
        Self { record }
    }

    pub fn route_targets(&self, bucket: u64, version: u64) -> Result<Vec<ShardId>, RoutingError> {
        self.route_targets_at_epoch(bucket, version, self.record.migration_epoch)
    }

    /// Route a replay or live write according to the migration epoch it carries.
    pub fn route_targets_at_epoch(
        &self,
        bucket: u64,
        version: u64,
        epoch: Epoch,
    ) -> Result<Vec<ShardId>, RoutingError> {
        let migrating = self.record.buckets.contains(bucket);
        let state = self.record.state;
        if migrating
            && matches!(
                state,
                MigrationState::DualWriting
                    | MigrationState::CatchingUp
                    | MigrationState::FencingOld
                    | MigrationState::Cutover
                    | MigrationState::Verifying
                    | MigrationState::GcEligible
                    | MigrationState::Done
            )
            && version != self.record.target_bucket_map_version
        {
            return Err(RoutingError::BucketMapVersionMismatch {
                bucket,
                expected: self.record.target_bucket_map_version,
                got: version,
            });
        }

        let targets = match state {
            MigrationState::DualWriting
            | MigrationState::CatchingUp
            | MigrationState::FencingOld
                if migrating && epoch >= self.record.migration_epoch =>
            {
                vec![self.record.donor_shards[0], self.record.recipient_shard]
            }
            MigrationState::DualWriting
            | MigrationState::CatchingUp
            | MigrationState::FencingOld
                if migrating =>
            {
                vec![self.record.donor_shards[0]]
            }
            MigrationState::Cutover
            | MigrationState::Verifying
            | MigrationState::GcEligible
            | MigrationState::Done
                if migrating
                    && epoch
                        >= self
                            .record
                            .cutover_epoch
                            .unwrap_or(self.record.migration_epoch) =>
            {
                vec![self.record.recipient_shard]
            }
            MigrationState::Cutover
            | MigrationState::Verifying
            | MigrationState::GcEligible
            | MigrationState::Done
                if migrating =>
            {
                vec![self.record.donor_shards[0]]
            }
            _ => vec![self.record.donor_shards[0]],
        };
        Ok(targets)
    }

    pub async fn apply_write(
        &self,
        write: &RoutedWrite,
        donor: &ShardDb,
        recipient: &ShardDb,
    ) -> Result<usize, RoutingError> {
        self.apply_write_at_epoch(write, self.record.migration_epoch, donor, recipient)
            .await
    }

    pub async fn apply_write_at_epoch(
        &self,
        write: &RoutedWrite,
        epoch: Epoch,
        donor: &ShardDb,
        recipient: &ShardDb,
    ) -> Result<usize, RoutingError> {
        let targets = self.route_targets_at_epoch(write.bucket, write.bucket_map_version, epoch)?;
        for target in &targets {
            let db = if *target == self.record.recipient_shard {
                recipient
            } else {
                donor
            };
            db.put(&write.key, &write.value)
                .await
                .expect("synthetic migration write should succeed");
        }
        Ok(targets.len())
    }
}
