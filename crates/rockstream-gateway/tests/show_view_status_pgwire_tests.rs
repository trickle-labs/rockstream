//! PGWire SQL tests for `SHOW VIEW STATUS` and `SHOW RESOURCE USAGE` (v0.53 Slice 6).

use std::sync::Arc;
use tokio_postgres::NoTls;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::view_lifecycle::ViewState;
use rockstream_types::workload::{FreshnessSlo, MemoryLimit, WorkloadDef};

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_gateway(catalog: CatalogStubs) -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').next_back().unwrap()
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn one_view_rows(
    state: ViewState,
    lag: rockstream_types::metrics::StageLagBreakdown,
    view_name: &str,
) -> Vec<Vec<Option<String>>> {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: view_name.to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.set_view_state(view_name, state);
    rockstream_types::metrics::set_view_stage_lag(view_name, lag);
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;
    simple_rows(&client, &format!("SHOW VIEW STATUS FOR {view_name};")).await
}

#[tokio::test]
async fn test_pgwire_show_view_status_all_views() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("analytics")
        .with_freshness_slo(FreshnessSlo::new(5000))
        .with_memory_limit(MemoryLimit::new(536870912));
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT id, count(*) FROM users GROUP BY id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("active_users", "analytics");

    catalog.add_view(CatalogView {
        name: "hourly_revenue".to_string(),
        sql: "SELECT hour, sum(amount) FROM orders GROUP BY hour".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].as_deref(), Some("public"));
    assert_eq!(rows[0][1].as_deref(), Some("active_users"));
    assert_eq!(rows[0][2].as_deref(), Some("RUNNING"));
    assert_eq!(rows[0][3].as_deref(), Some("analytics"));
    assert_eq!(rows[0][4].as_deref(), Some("5000"));
    assert_eq!(rows[0][5].as_deref(), Some("536870912"));

    assert_eq!(rows[1][1].as_deref(), Some("hourly_revenue"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_single_view() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR active_users;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1].as_deref(), Some("active_users"));
    assert_eq!(rows[0][2].as_deref(), Some("RUNNING"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_namespace() {
    let catalog = CatalogStubs::new();
    catalog.add_view_in_namespace(CatalogView {
        name: "prod_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "production".to_string(),
        op_id: None,
    });
    catalog.add_view_in_namespace(CatalogView {
        name: "dev_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "development".to_string(),
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR NAMESPACE production;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("production"));
    assert_eq!(rows[0][1].as_deref(), Some("prod_view"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_empty() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_pgwire_show_view_status_not_found_rs1001() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW VIEW STATUS FOR nonexistent_view;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1001"), "expected RS-1001 in error: {msg}");
    assert!(msg.contains("nonexistent_view"));
}

#[tokio::test]
async fn test_pgwire_show_view_status_namespace_not_found() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW VIEW STATUS FOR NAMESPACE nonexistent_ns;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1001"), "expected RS-1001 in error: {msg}");
    assert!(msg.contains("nonexistent_ns"));
}

#[tokio::test]
async fn test_pgwire_show_resource_usage() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("active_users"));
}

#[tokio::test]
async fn test_pgwire_show_resource_usage_for_workload() {
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("analytics");
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("active_users", "analytics");

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW RESOURCE USAGE FOR WORKLOAD analytics;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("analytics"));
}

#[tokio::test]
async fn test_pgwire_show_cluster_resource_usage() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW CLUSTER RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("cluster"));
}

#[tokio::test]
async fn test_pgwire_show_resource_usage_empty() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_pgwire_show_resource_usage_workload_not_found() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let err = client
        .simple_query("SHOW RESOURCE USAGE FOR WORKLOAD nonexistent_wl;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1005"), "expected RS-1005 in error: {msg}");
}

#[tokio::test]
async fn test_pgwire_show_cluster_resource_usage_empty() {
    let catalog = CatalogStubs::new();
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW CLUSTER RESOURCE USAGE;").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_pgwire_show_view_status_and_resource_usage() {
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("analytics")
        .with_freshness_slo(FreshnessSlo::new(5000))
        .with_memory_limit(MemoryLimit::new(536870912));
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT id, count(*) FROM users GROUP BY id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("active_users", "analytics");

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let status_rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(status_rows.len(), 1);

    let res_rows = simple_rows(&client, "SHOW RESOURCE USAGE;").await;
    assert_eq!(res_rows.len(), 1);

    let cluster_rows = simple_rows(&client, "SHOW CLUSTER RESOURCE USAGE;").await;
    assert_eq!(cluster_rows.len(), 1);
}

#[tokio::test]
async fn test_pgwire_show_view_status_decomposed_lag_all_views() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let catalog = CatalogStubs::new();
    let wl = WorkloadDef::new("realtime")
        .with_freshness_slo(FreshnessSlo::new(1000))
        .with_memory_limit(MemoryLimit::new(1048576));
    catalog.add_workload(wl);
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT * FROM orders".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("orders_mv", "realtime");

    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 12,
        decode_lag_ms: 3,
        compute_lag_ms: 25,
        alignment_lag_ms: 4,
        sink_lag_ms: 10,
        spill_lag_ms: 2,
        storage_pressure_ms: 1,
        total_lag_ms: 57,
    };
    rockstream_types::metrics::set_view_stage_lag("orders_mv", lag);

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS;").await;
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r[0].as_deref(), Some("public"));
    assert_eq!(r[1].as_deref(), Some("orders_mv"));
    assert_eq!(r[2].as_deref(), Some("RUNNING"));
    assert_eq!(r[3].as_deref(), Some("realtime"));
    assert_eq!(r[4].as_deref(), Some("1000"));
    assert_eq!(r[5].as_deref(), Some("1048576"));
    assert_eq!(r[6].as_deref(), Some("-"));
    assert_eq!(r[7].as_deref(), Some("12")); // source_lag_ms
    assert_eq!(r[8].as_deref(), Some("3")); // decode_lag_ms
    assert_eq!(r[9].as_deref(), Some("25")); // compute_lag_ms
    assert_eq!(r[10].as_deref(), Some("4")); // alignment_lag_ms
    assert_eq!(r[11].as_deref(), Some("10")); // sink_lag_ms
    assert_eq!(r[12].as_deref(), Some("2")); // spill_lag_ms
    assert_eq!(r[13].as_deref(), Some("1")); // storage_pressure_ms
    assert_eq!(r[14].as_deref(), Some("57")); // total_lag_ms
    assert_eq!(r[15].as_deref(), Some("spilling"));
    assert_eq!(r[16].as_deref(), Some("RS-3703"));
    assert_eq!(r[17].as_deref(), Some("compute_lag"));
    assert_eq!(r[18].as_deref(), None);
    assert_eq!(r[19].as_deref(), None);
    assert_eq!(r[20].as_deref(), None);
    assert_eq!(r[21].as_deref(), None);
}

#[tokio::test]
async fn test_pgwire_show_view_status_decomposed_lag_single_view() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "events_mv".to_string(),
        sql: "SELECT * FROM events".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 5,
        decode_lag_ms: 1,
        compute_lag_ms: 8,
        alignment_lag_ms: 2,
        sink_lag_ms: 4,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 20,
    };
    rockstream_types::metrics::set_view_stage_lag("events_mv", lag);

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR events_mv;").await;
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r[1].as_deref(), Some("events_mv"));
    assert_eq!(r[7].as_deref(), Some("5"));
    assert_eq!(r[8].as_deref(), Some("1"));
    assert_eq!(r[9].as_deref(), Some("8"));
    assert_eq!(r[10].as_deref(), Some("2"));
    assert_eq!(r[11].as_deref(), Some("4"));
    assert_eq!(r[12].as_deref(), Some("0"));
    assert_eq!(r[13].as_deref(), Some("0"));
    assert_eq!(r[14].as_deref(), Some("20"));
    assert_eq!(r[15].as_deref(), Some("sink_blocked"));
    assert_eq!(r[16].as_deref(), Some("RS-3706"));
    assert_eq!(r[17].as_deref(), Some("compute_lag"));
    assert_eq!(r[18].as_deref(), None);
    assert_eq!(r[19].as_deref(), None);
    assert_eq!(r[20].as_deref(), None);
    assert_eq!(r[21].as_deref(), None);

    // Negative test: non-existent view returns RS-1001
    let err = client
        .simple_query("SHOW VIEW STATUS FOR nonexistent_view;")
        .await
        .unwrap_err();
    let msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(msg.contains("RS-1001"), "expected RS-1001 in error: {msg}");
}

#[tokio::test]
async fn test_show_view_status_reason_schema_all_forms() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "active_users".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 3,
        decode_lag_ms: 1,
        compute_lag_ms: 2,
        alignment_lag_ms: 0,
        sink_lag_ms: 0,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 6,
    };
    rockstream_types::metrics::set_view_stage_lag("active_users", lag);
    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;
    for sql in [
        "SHOW VIEW STATUS;",
        "SHOW VIEW STATUS FOR active_users;",
        "SHOW VIEW STATUS FOR NAMESPACE public;",
    ] {
        let rows = simple_rows(&client, sql).await;
        assert_eq!(rows.len(), 1, "{sql}");
        assert_eq!(rows[0].len(), 22, "{sql}");
        assert_eq!(rows[0][15].as_deref(), Some("waiting_on_source"), "{sql}");
        assert_eq!(rows[0][16].as_deref(), Some("RS-3701"), "{sql}");
        assert_eq!(rows[0][17].as_deref(), Some("source_lag"), "{sql}");
    }
}

#[tokio::test]
async fn test_reason_waiting_on_source_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 7,
        decode_lag_ms: 0,
        compute_lag_ms: 0,
        alignment_lag_ms: 0,
        sink_lag_ms: 0,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 7,
    };
    let rows = one_view_rows(ViewState::Running, lag, "reason_source_mv").await;
    assert_eq!(rows[0][15].as_deref(), Some("waiting_on_source"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3701"));
    assert_eq!(rows[0][17].as_deref(), Some("source_lag"));
}

#[tokio::test]
async fn test_reason_quota_admission_rejected_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 2,
        decode_lag_ms: 2,
        compute_lag_ms: 2,
        alignment_lag_ms: 2,
        sink_lag_ms: 2,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 10,
    };
    let rows = one_view_rows(ViewState::OverBudgetRejected, lag, "reason_quota_mv").await;
    assert_eq!(rows[0][15].as_deref(), Some("quota_admission_rejected"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3702"));
}

#[tokio::test]
async fn test_reason_spilling_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 1,
        compute_lag_ms: 1,
        alignment_lag_ms: 1,
        sink_lag_ms: 1,
        spill_lag_ms: 8,
        storage_pressure_ms: 0,
        total_lag_ms: 13,
    };
    let rows = one_view_rows(ViewState::Running, lag, "reason_spill_mv").await;
    assert_eq!(rows[0][15].as_deref(), Some("spilling"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3703"));
    assert_eq!(rows[0][17].as_deref(), Some("spill_lag"));
}

#[tokio::test]
async fn test_reason_over_budget_relaxed_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 1,
        compute_lag_ms: 9,
        alignment_lag_ms: 1,
        sink_lag_ms: 1,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 13,
    };
    let rows = one_view_rows(ViewState::OverBudgetRelaxed, lag, "reason_over_budget_mv").await;
    assert_eq!(rows[0][15].as_deref(), Some("over_budget_relaxed"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3704"));
}

#[tokio::test]
async fn test_reason_checkpoint_alignment_stalled_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 1,
        compute_lag_ms: 1,
        alignment_lag_ms: 9,
        sink_lag_ms: 0,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 12,
    };
    let rows = one_view_rows(ViewState::Running, lag, "reason_alignment_mv").await;
    assert_eq!(rows[0][15].as_deref(), Some("checkpoint_alignment_stalled"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3705"));
    assert_eq!(rows[0][17].as_deref(), Some("alignment_lag"));
}

#[tokio::test]
async fn test_reason_sink_blocked_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 1,
        compute_lag_ms: 1,
        alignment_lag_ms: 1,
        sink_lag_ms: 9,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 13,
    };
    let rows = one_view_rows(ViewState::Running, lag, "reason_sink_mv").await;
    assert_eq!(rows[0][15].as_deref(), Some("sink_blocked"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3706"));
    assert_eq!(rows[0][17].as_deref(), Some("sink_lag"));
}

#[tokio::test]
async fn test_reason_topology_transition_migration_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 1,
        compute_lag_ms: 5,
        alignment_lag_ms: 1,
        sink_lag_ms: 1,
        spill_lag_ms: 0,
        storage_pressure_ms: 12,
        total_lag_ms: 21,
    };
    let rows = one_view_rows(ViewState::Paused, lag, "reason_migration_mv").await;
    assert_eq!(
        rows[0][15].as_deref(),
        Some("topology_transition_in_progress")
    );
    assert_eq!(rows[0][16].as_deref(), Some("RS-3707"));
    assert_eq!(rows[0][18].as_deref(), Some("shard_migration"));
    assert_eq!(rows[0][19].as_deref(), Some("12"));
    assert_eq!(rows[0][20].as_deref(), Some("5"));
    assert_eq!(rows[0][21].as_deref(), Some("21"));
}

#[tokio::test]
async fn test_reason_topology_transition_drain_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 4,
        compute_lag_ms: 1,
        alignment_lag_ms: 1,
        sink_lag_ms: 6,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 13,
    };
    let rows = one_view_rows(ViewState::Paused, lag, "reason_drain_mv").await;
    assert_eq!(
        rows[0][15].as_deref(),
        Some("topology_transition_in_progress")
    );
    assert_eq!(rows[0][16].as_deref(), Some("RS-3707"));
    assert_eq!(rows[0][18].as_deref(), Some("worker_drain"));
    assert_eq!(rows[0][19].as_deref(), Some("6"));
    assert_eq!(rows[0][20].as_deref(), Some("4"));
    assert_eq!(rows[0][21].as_deref(), Some("13"));
}

#[tokio::test]
async fn test_reason_recovering_pgwire_and_cli() {
    let _g = TEST_LOCK.lock().await;
    rockstream_types::metrics::reset_all();
    let lag = rockstream_types::metrics::StageLagBreakdown {
        source_lag_ms: 1,
        decode_lag_ms: 2,
        compute_lag_ms: 9,
        alignment_lag_ms: 0,
        sink_lag_ms: 0,
        spill_lag_ms: 0,
        storage_pressure_ms: 0,
        total_lag_ms: 12,
    };
    let rows = one_view_rows(
        ViewState::BackfillingFromEpoch(42),
        lag,
        "reason_recovering_mv",
    )
    .await;
    assert_eq!(rows[0][15].as_deref(), Some("recovering"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3708"));
    assert_eq!(rows[0][17].as_deref(), Some("compute_lag"));
}
