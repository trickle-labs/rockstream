//! Tests for client trait separation and anti-fabrication truth guarantees (v0.60 Slices 1 & 2).

use rockstream_cli::transport::{
    CatalogApi, ClientIdentity, OperationApi, RemoteCatalogClient, RemoteOperationClient,
    RemoteStorageAdminClient, RemoteTopologyClient, StorageAdminApi, TopologyApi,
};
use rockstream_types::error_code::RS_0004;
use std::path::PathBuf;

#[test]
fn test_api_traits_separate_remote_from_mock_implementations() {
    let identity = ClientIdentity::new("truth_tester");
    let remote_topo =
        RemoteTopologyClient::new(Some("127.0.0.1:59999".to_string()), identity.clone());
    let remote_op =
        RemoteOperationClient::new(Some("127.0.0.1:59999".to_string()), identity.clone());
    let remote_catalog =
        RemoteCatalogClient::new(Some("127.0.0.1:59999".to_string()), identity.clone());
    let remote_storage = RemoteStorageAdminClient::with_identity(identity);

    // Verify trait bounds
    fn assert_topology_api<T: TopologyApi + ?Sized>(_c: &T) {}
    fn assert_operation_api<T: OperationApi + ?Sized>(_c: &T) {}
    fn assert_catalog_api<T: CatalogApi + ?Sized>(_c: &T) {}
    fn assert_storage_admin_api<T: StorageAdminApi + ?Sized>(_c: &T) {}

    assert_topology_api(&remote_topo);
    assert_operation_api(&remote_op);
    assert_catalog_api(&remote_catalog);
    assert_storage_admin_api(&remote_storage);
}

#[test]
fn test_unreachable_control_fails_closed_with_rs0004_and_zero_data() {
    let identity = ClientIdentity::new("truth_tester");
    let remote_topo = RemoteTopologyClient::new(Some("127.0.0.1:59999".to_string()), identity);

    let err = remote_topo.cluster_status().unwrap_err();
    assert_eq!(err.code, RS_0004);
    assert!(
        err.message
            .contains("cannot reach RockStream control service")
            || err.message.contains("failed to reach control plane"),
        "unexpected error message: {}",
        err.message
    );
    assert!(
        err.next_steps
            .contains("verify `rockstream start` is running")
            || err.next_steps.contains("verify the control service URL")
            || err.next_steps.contains("Verify the control service URL"),
        "unexpected next steps: {}",
        err.next_steps
    );

    // list_workers must also fail closed with RS-0004 (no fabricated workers)
    let workers_err = remote_topo.list_workers().unwrap_err();
    assert_eq!(workers_err.code, RS_0004);

    // list_shards must fail closed with RS-0004 (no fabricated shards)
    let shards_err = remote_topo.list_shards().unwrap_err();
    assert_eq!(shards_err.code, RS_0004);

    // cluster_quotas must fail closed with RS-0004 (no fabricated quotas)
    let quotas_err = remote_topo.cluster_quotas().unwrap_err();
    assert_eq!(quotas_err.code, RS_0004);
}

#[test]
fn test_catalog_client_never_fabricates_views() {
    let identity = ClientIdentity::new("truth_tester");
    let remote_catalog = RemoteCatalogClient::new(Some("127.0.0.1:59999".to_string()), identity);

    // Must fail closed with RS-0004 when remote is unreachable
    let err = remote_catalog.list_views().unwrap_err();
    assert_eq!(err.code, RS_0004);

    let src_err = remote_catalog.list_sources().unwrap_err();
    assert_eq!(src_err.code, RS_0004);

    let schemas_err = remote_catalog.list_schemas().unwrap_err();
    assert_eq!(schemas_err.code, RS_0004);
}

#[test]
fn test_topology_client_never_fabricates_topology() {
    let identity = ClientIdentity::new("truth_tester");
    // Even without an explicit control address, it must not return fake cluster status
    let remote_topo = RemoteTopologyClient::new(None, identity);
    let err = remote_topo.cluster_status().unwrap_err();
    assert_eq!(err.code, RS_0004);
}

#[test]
fn test_operation_client_never_fabricates_operations() {
    let identity = ClientIdentity::new("admin");
    let remote_op = RemoteOperationClient::new(Some("127.0.0.1:59999".to_string()), identity);

    let err = remote_op.drain_worker(1).unwrap_err();
    assert_eq!(err.code, RS_0004);

    let mig_err = remote_op.migrate_shard(1, 2).unwrap_err();
    assert_eq!(mig_err.code, RS_0004);
}

#[test]
fn test_storage_admin_client_never_fabricates_state() {
    let identity = ClientIdentity::new("admin");
    let remote_storage = RemoteStorageAdminClient::with_identity(identity);

    let non_existent = PathBuf::from("/non/existent/path/for/rockstream/storage");
    let err = remote_storage
        .show_checkpoint(&non_existent, 1)
        .unwrap_err();
    // Non-existent checkpoint or storage must error, never return mock alignment
    assert!(err.code.value() > 0);
}
