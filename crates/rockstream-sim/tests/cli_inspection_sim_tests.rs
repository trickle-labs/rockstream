//! Simulation tests for CLI continuous polling under simulated faults (v0.53 Slice 7).

use std::time::Duration;

use rockstream_cli::output::OutputFormat;
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient};
use rockstream_cli::{run_cluster_status, run_resource_usage, run_shard_list, run_view_status};
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};

#[tokio::test]
async fn test_cli_continuous_polling_under_simulated_faults() {
    buggify_init(12345);

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());

    let handle = service.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let client_identity = ClientIdentity::default();
    let cli_control = ControlClient::new(Some(control_url), client_identity);
    let cli_catalog = CatalogClient::with_defaults();

    // Loop with buggify fault injection at control service query dispatch and status collection
    for _ in 0..50 {
        let inject_jitter = buggify!("cli.poll.jitter", 0.3);
        if inject_jitter {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Poll CLI inspection routines
        let status = run_cluster_status(OutputFormat::Json, &cli_control);
        assert!(
            status.is_ok(),
            "cluster status query must succeed: {status:?}"
        );

        let view_status = run_view_status(OutputFormat::Json, &cli_catalog, None);
        assert!(
            view_status.is_ok(),
            "view status query must succeed: {view_status:?}"
        );

        let res_usage = run_resource_usage(OutputFormat::Json, &cli_catalog, None);
        assert!(
            res_usage.is_ok(),
            "resource usage query must succeed: {res_usage:?}"
        );

        let shard_list = run_shard_list(OutputFormat::Json, &cli_control);
        assert!(
            shard_list.is_ok(),
            "shard list query must succeed: {shard_list:?}"
        );
    }

    buggify_disable();
}
