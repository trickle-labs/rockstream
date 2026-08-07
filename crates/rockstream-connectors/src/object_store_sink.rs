//! Object-store sink using conditional S3-compatible writes.

use std::collections::BTreeSet;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions};
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile, SinkState};
use rockstream_types::timestamp::Epoch;

use crate::sink_connector::{
    assert_commit_pointer_atomic, assert_epoch_committed_only_after_cluster_checkpoint,
    assert_no_duplicate_delivery, assert_recovery_dispatch_idempotent, SinkConnector, SinkError,
};

#[cfg(feature = "simulation")]
use rockstream_sim::buggify;

pub const OBJECT_STORE_SINK_MAX_PENDING_EPOCHS: usize = 5;

/// Synchronous `SinkConnector` adapter over a real object-store client.
pub struct ObjectStoreSink {
    connector_id: ConnectorId,
    store: Arc<dyn ObjectStore>,
    runtime: tokio::runtime::Runtime,
    delivered_epochs: BTreeSet<Epoch>,
    max_pending_epochs: usize,
    pending_epochs_count: usize,
    cluster_committed: Epoch,
    #[cfg(feature = "simulation")]
    partial_write_probability: f64,
}

impl ObjectStoreSink {
    pub fn new(connector_id: ConnectorId, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            connector_id,
            store,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("object-store sink runtime must initialize"),
            delivered_epochs: BTreeSet::new(),
            max_pending_epochs: OBJECT_STORE_SINK_MAX_PENDING_EPOCHS,
            pending_epochs_count: 0,
            cluster_committed: 0,
            #[cfg(feature = "simulation")]
            partial_write_probability: 0.0,
        }
    }

    pub fn set_cluster_committed(&mut self, epoch: Epoch) {
        self.cluster_committed = epoch;
    }

    #[cfg(feature = "simulation")]
    pub fn set_partial_write_probability(&mut self, probability: f64) {
        self.partial_write_probability = probability.clamp(0.0, 1.0);
    }

    pub fn object_store_sink_pending_epochs_count(&self) -> usize {
        self.pending_epochs_count
    }

    pub fn backpressure_active(&self) -> bool {
        self.pending_epochs_count >= self.max_pending_epochs
    }

    pub fn final_exists(&self, epoch: Epoch) -> bool {
        self.runtime
            .block_on(self.store.head(&Self::final_path(epoch)))
            .is_ok()
    }

    pub fn final_len(&self, epoch: Epoch) -> Option<usize> {
        self.final_bytes(epoch).ok().map(|bytes| bytes.len())
    }

    pub fn pending_exists(&self, epoch: Epoch) -> bool {
        self.runtime
            .block_on(self.store.head(&Self::pending_path(epoch)))
            .is_ok()
    }

    fn pending_path(epoch: Epoch) -> Path {
        Path::from(format!("_pending/{epoch}/part-0"))
    }

    fn final_path(epoch: Epoch) -> Path {
        Path::from(format!("final/{epoch}/part-0"))
    }

    fn final_bytes(&self, epoch: Epoch) -> Result<bytes::Bytes, SinkError> {
        self.runtime
            .block_on(async {
                self.store
                    .get(&Self::final_path(epoch))
                    .await?
                    .bytes()
                    .await
            })
            .map_err(|err| SinkError::Io(err.to_string()))
    }

    fn pending_bytes(&self, epoch: Epoch) -> Result<bytes::Bytes, SinkError> {
        self.runtime
            .block_on(async {
                self.store
                    .get(&Self::pending_path(epoch))
                    .await?
                    .bytes()
                    .await
            })
            .map_err(|err| SinkError::CommitFailed {
                epoch,
                reason: err.to_string(),
            })
    }

    fn finalize(&mut self, epoch: Epoch, bytes: bytes::Bytes) -> Result<(), SinkError> {
        let final_path = Self::final_path(epoch);
        if let Ok(final_bytes) = self.final_bytes(epoch) {
            if final_bytes != bytes {
                return Err(SinkError::CommitFailed {
                    epoch,
                    reason: "conditional final write conflict: final bytes differ".to_owned(),
                });
            }
            self.runtime
                .block_on(self.store.delete(&Self::pending_path(epoch)))
                .map_err(|err| SinkError::CommitFailed {
                    epoch,
                    reason: err.to_string(),
                })?;
            self.pending_epochs_count = self.pending_epochs_count.saturating_sub(1);
            self.delivered_epochs.insert(epoch);
            return Ok(());
        }
        #[cfg(feature = "simulation")]
        let bytes_to_write = if self.partial_write_probability > 0.0
            && buggify!("object_store.partial_write", self.partial_write_probability)
        {
            bytes.slice(..bytes.len() / 2)
        } else {
            bytes.clone()
        };
        #[cfg(not(feature = "simulation"))]
        let bytes_to_write = bytes.clone();
        match self.runtime.block_on(self.store.put_opts(
            &final_path,
            bytes_to_write.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )) {
            Ok(_) => {}
            Err(_)
                if self
                    .final_bytes(epoch)
                    .is_ok_and(|final_bytes| final_bytes == bytes) => {}
            Err(_) => {
                return Err(SinkError::CommitFailed {
                    epoch,
                    reason: "conditional final write conflict: final bytes differ".to_owned(),
                });
            }
        }
        let observed = self.final_bytes(epoch)?;
        assert_commit_pointer_atomic(self.connector_id, epoch, observed.len(), bytes.len());
        self.runtime
            .block_on(self.store.delete(&Self::pending_path(epoch)))
            .map_err(|err| SinkError::CommitFailed {
                epoch,
                reason: err.to_string(),
            })?;
        self.pending_epochs_count = self.pending_epochs_count.saturating_sub(1);
        self.delivered_epochs.insert(epoch);
        Ok(())
    }
}

impl SinkConnector for ObjectStoreSink {
    fn idempotency_profile(&self) -> SinkIdempotencyProfile {
        SinkIdempotencyProfile::NativeIdempotent
    }

    fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError> {
        if self.backpressure_active() {
            return Err(SinkError::PreCommitFailed {
                epoch,
                reason: format!(
                    "backpressure: pending_epochs={} >= max={}",
                    self.pending_epochs_count, self.max_pending_epochs
                ),
            });
        }
        self.runtime
            .block_on(self.store.put(
                &Self::pending_path(epoch),
                vec![0xAB; row_count.max(1)].into(),
            ))
            .map_err(|err| SinkError::PreCommitFailed {
                epoch,
                reason: err.to_string(),
            })?;
        self.pending_epochs_count += 1;
        Ok(SinkState::PreCommitted {
            staged_rows: row_count,
            pending_handle: format!("_pending/{epoch}/part-0").into_bytes(),
        })
    }

    fn commit(&mut self, epoch: Epoch, state: &SinkState) -> Result<(), SinkError> {
        assert_epoch_committed_only_after_cluster_checkpoint(
            self.connector_id,
            epoch,
            self.cluster_committed,
        );
        if matches!(state, SinkState::Idle) {
            return Err(SinkError::CommitFailed {
                epoch,
                reason: "commit called on Idle sink state".to_owned(),
            });
        }
        if self.final_exists(epoch) && !self.pending_exists(epoch) {
            return Ok(());
        }
        assert_no_duplicate_delivery(self.connector_id, epoch, &self.delivered_epochs);
        self.finalize(epoch, self.pending_bytes(epoch)?)
    }

    fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError> {
        if self.pending_exists(epoch) {
            self.runtime
                .block_on(self.store.delete(&Self::pending_path(epoch)))
                .map_err(|err| SinkError::Io(err.to_string()))?;
            self.pending_epochs_count = self.pending_epochs_count.saturating_sub(1);
        }
        Ok(())
    }

    fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError> {
        let RecoveryAction::RerunCommit { epoch, .. } = action.clone() else {
            return Ok(());
        };
        if self.final_exists(epoch) {
            if self.pending_exists(epoch) {
                let pending = self.pending_bytes(epoch)?;
                if self.final_bytes(epoch)? != pending {
                    self.runtime
                        .block_on(self.store.delete(&Self::final_path(epoch)))
                        .map_err(|err| SinkError::Io(err.to_string()))?;
                } else {
                    self.runtime
                        .block_on(self.store.delete(&Self::pending_path(epoch)))
                        .map_err(|err| SinkError::Io(err.to_string()))?;
                    self.delivered_epochs.insert(epoch);
                    assert_recovery_dispatch_idempotent(
                        self.connector_id,
                        &action,
                        &SinkState::Committed,
                    );
                    return Ok(());
                }
            } else {
                self.delivered_epochs.insert(epoch);
                assert_recovery_dispatch_idempotent(
                    self.connector_id,
                    &action,
                    &SinkState::Committed,
                );
                return Ok(());
            }
        }
        self.finalize(epoch, self.pending_bytes(epoch)?)?;
        assert_recovery_dispatch_idempotent(self.connector_id, &action, &SinkState::Committed);
        Ok(())
    }
}
