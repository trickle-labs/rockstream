//! RockStream worker runtime.
//!
//! v0.4 re-exports the key runtime primitives from `rockstream-ops`:
//! the `CreditScheduler`, `EmbeddedRuntime`, and related types.
//!
//! Later versions add the epoch-commit coordinator (v0.5), exchange (v0.16),
//! and the control-plane service (v0.15).

pub use rockstream_ops::embedded::{EmbeddedCounters, EmbeddedRuntime};
pub use rockstream_ops::scheduler::CreditScheduler;
pub use rockstream_ops::task::{OperatorTask, OPERATOR_CHANNEL_CAPACITY};

pub mod arrangement_attach;
pub mod client;
pub mod data_plane;
pub mod secrets;
pub mod tls;
pub use client::{
    start_worker_client, start_worker_client_with_metadata, start_worker_client_with_tls,
    start_worker_client_with_tls_and_metadata, ShardState, WorkerClientHandle,
};
pub use data_plane::DataPlaneClient;
pub use secrets::{
    ResolvedSecret, SecretManagerError, WorkerSecretManager, MAX_WORKER_SECRET_TOKENS,
};

pub mod exchange;
pub mod migration;
pub use migration::{DualWriteRouter, RoutedWrite, RoutingError};

pub mod fence;
pub use fence::{
    assert_single_lease_holder, assert_valid_writer, SelfFenceGuard, DEFAULT_SELF_FENCE_DEADLINE,
};

pub mod recovery;
pub use recovery::{
    RecoveryDriver, RecoveryError, RecoveryProgress, DEFAULT_SHARD_RECOVERY_BUDGET,
};

pub mod quota;
pub use quota::WorkerQuotaManager;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_crate_compiles() {
        let rt = EmbeddedRuntime::new();
        assert_eq!(rt.counters.grpc_call_count(), 0);
        assert_eq!(rt.counters.shuffle_write_count(), 0);
    }

    #[tokio::test]
    async fn test_worker_registration_and_heartbeats() {
        let catalog = rockstream_control::TopologyCatalog::new();
        let manager = rockstream_control::ShardManager::new();
        let svc =
            rockstream_control::ControlService::new(catalog.clone()).with_shard_manager(manager);

        let handle = svc.start("127.0.0.1:0").await.unwrap();
        let control_url = handle.addr.to_string();

        let storage_dir = tempfile::tempdir().unwrap();

        let (client, worker_handle) = start_worker_client(42, &control_url, storage_dir.path())
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(
            client.worker_id(),
            Some(rockstream_types::ids::WorkerId(42))
        );
        assert_eq!(catalog.len(), 1);

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        worker_handle.abort();
        handle.shutdown();
    }
}
