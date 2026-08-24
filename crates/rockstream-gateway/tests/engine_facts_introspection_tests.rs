//! PGWire SQL tests for stable raw engine facts in `SHOW VIEW STATUS` and `EXPLAIN INCREMENTAL` (v0.59.10 OBS-03).

use std::sync::Arc;
use tokio_postgres::NoTls;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_plan::PlanNode;
use rockstream_sql::explain_incremental_with_engine_facts;
use rockstream_types::explain::ViewEngineFacts;
use rockstream_types::ids::ArrangementId;

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
        .expect("query failed")
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(str::to_string));
                }
                Some(values)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_arrangement_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "orders_summary".to_string(),
        sql: "SELECT cust_id, sum(total) FROM orders GROUP BY cust_id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        arrangement_id: Some(ArrangementId(2048)),
        consumer_count: 4,
        shared_state_bytes: 8388608,
        bytes_saved_by_sharing: 25165824,
        delta_amplification: 1.05,
        join_amplification: 1.0,
        merge_operand_count: 5000,
        dirty_key_count: 1200,
        logical_write_bytes: 409600,
        physical_write_amplification: 1.45,
        hot_key_bucket_count: 8,
        factorization_strategy: "Classic".to_string(),
        predicate_filter_selectivity: 0.85,
        cache_hit_rate: 0.98,
        epoch_group_size: 2,
        checkpoint_mode: "Changelog".to_string(),
        compaction_debt: 1048576,
        degradation_reason: "None".to_string(),
        reason_code: "OK".to_string(),
        dominant_contributor: "none".to_string(),
        source_lag_ms: 12,
        compute_lag_ms: 5,
        spill_bytes: 0,
        checkpoint_id: 42,
        frontier: "500".to_string(),
        recommended_action_key: "none".to_string(),
    };
    catalog.set_view_engine_facts("orders_summary", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR orders_summary;").await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 44);

    // Verify arrangement facts
    assert_eq!(row[22].as_deref(), Some("arr-2048"));
    assert_eq!(row[23].as_deref(), Some("4"));
    assert_eq!(row[24].as_deref(), Some("8388608"));
    assert_eq!(row[25].as_deref(), Some("25165824"));
    assert_eq!(row[26].as_deref(), Some("500"));

    // Verify explain rendering with facts
    let plan = PlanNode::Source {
        name: "orders".to_string(),
    };
    let explain_text = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain_text.contains("arrangement_id=arr-2048"));
    assert!(explain_text.contains("consumers=4"));
    assert!(explain_text.contains("shared_bytes=8388608"));
    assert!(explain_text.contains("saved_bytes=25165824"));
}

#[tokio::test]
async fn test_equivalent_shared_views_report_one_arrangement() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "view_a".to_string(),
        sql: "SELECT item_id, count(*) FROM sales GROUP BY item_id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.add_view(CatalogView {
        name: "view_b".to_string(),
        sql: "SELECT item_id, count(*) FROM sales GROUP BY item_id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let shared_facts = ViewEngineFacts {
        arrangement_id: Some(ArrangementId(999)),
        consumer_count: 2,
        shared_state_bytes: 4194304,
        bytes_saved_by_sharing: 4194304,
        frontier: "123".to_string(),
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("view_a", shared_facts.clone());
    catalog.set_view_engine_facts("view_b", shared_facts);

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows_a = simple_rows(&client, "SHOW VIEW STATUS FOR view_a;").await;
    let rows_b = simple_rows(&client, "SHOW VIEW STATUS FOR view_b;").await;

    assert_eq!(rows_a[0][22], rows_b[0][22]); // arrangement_id
    assert_eq!(rows_a[0][23].as_deref(), Some("2")); // consumer_count
    assert_eq!(rows_b[0][23].as_deref(), Some("2"));
    assert_eq!(rows_a[0][42], rows_b[0][42]); // frontier
}

#[tokio::test]
async fn test_amplification_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "joined_view".to_string(),
        sql: "SELECT a.id FROM a JOIN b ON a.id = b.id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        delta_amplification: 1.15,
        join_amplification: 2.30,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("joined_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR joined_view;").await;
    assert_eq!(rows[0][27].as_deref(), Some("1.15")); // delta_amplification
    assert_eq!(rows[0][28].as_deref(), Some("2.30")); // join_amplification

    let plan = PlanNode::Source {
        name: "a".to_string(),
    };
    let explain_text = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain_text.contains("delta_amp=1.15"));
    assert!(explain_text.contains("join_amp=2.30"));
}

#[tokio::test]
async fn test_mutation_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "mut_view".to_string(),
        sql: "SELECT id FROM items".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        merge_operand_count: 8888,
        dirty_key_count: 4444,
        logical_write_bytes: 1048576,
        physical_write_amplification: 1.25,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("mut_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR mut_view;").await;
    assert_eq!(rows[0][29].as_deref(), Some("8888"));
    assert_eq!(rows[0][30].as_deref(), Some("4444"));
    assert_eq!(rows[0][31].as_deref(), Some("1048576"));
    assert_eq!(rows[0][32].as_deref(), Some("1.25"));

    let plan = PlanNode::Source {
        name: "items".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("merge_operands=8888"));
    assert!(explain.contains("dirty_keys=4444"));
    assert!(explain.contains("logical_bytes=1048576"));
    assert!(explain.contains("physical_write_amp=1.25"));
}

#[tokio::test]
async fn test_skew_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "skew_view".to_string(),
        sql: "SELECT key, sum(val) FROM t GROUP BY key".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        hot_key_bucket_count: 16,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("skew_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR skew_view;").await;
    assert_eq!(rows[0][33].as_deref(), Some("16"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("hot_keys=16"));
}

#[tokio::test]
async fn test_plan_strategy_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "strategy_view".to_string(),
        sql: "SELECT a FROM t WHERE a > 10".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        factorization_strategy: "Factorized".to_string(),
        predicate_filter_selectivity: 0.42,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("strategy_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR strategy_view;").await;
    assert_eq!(rows[0][34].as_deref(), Some("Factorized"));
    assert_eq!(rows[0][35].as_deref(), Some("0.42"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("strategy=Factorized"));
    assert!(explain.contains("selectivity=0.42"));
}

#[tokio::test]
async fn test_cache_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "cache_view".to_string(),
        sql: "SELECT a FROM t".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        cache_hit_rate: 0.94,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("cache_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR cache_view;").await;
    assert_eq!(rows[0][36].as_deref(), Some("0.94"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("cache_hit_rate=0.94"));
}

#[tokio::test]
async fn test_runtime_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "rt_view".to_string(),
        sql: "SELECT a FROM t".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        epoch_group_size: 4,
        spill_bytes: 65536,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("rt_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR rt_view;").await;
    assert_eq!(rows[0][37].as_deref(), Some("4"));
    assert_eq!(rows[0][40].as_deref(), Some("65536"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("epoch_group=4"));
    assert!(explain.contains("spill_bytes=65536"));
}

#[tokio::test]
async fn test_checkpoint_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "cp_view".to_string(),
        sql: "SELECT a FROM t".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        checkpoint_mode: "Aligned".to_string(),
        checkpoint_id: 101,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("cp_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR cp_view;").await;
    assert_eq!(rows[0][38].as_deref(), Some("Aligned"));
    assert_eq!(rows[0][41].as_deref(), Some("101"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("checkpoint_mode=Aligned"));
    assert!(explain.contains("checkpoint_id=101"));
}

#[tokio::test]
async fn test_health_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "health_view".to_string(),
        sql: "SELECT a FROM t".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        degradation_reason: "HighCompactionDebt".to_string(),
        reason_code: "RS-3001".to_string(),
        dominant_contributor: "compactor".to_string(),
        recommended_action_key: "increase_compaction_threads".to_string(),
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("health_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR health_view;").await;
    assert_eq!(rows[0][15].as_deref(), Some("HighCompactionDebt"));
    assert_eq!(rows[0][16].as_deref(), Some("RS-3001"));
    assert_eq!(rows[0][17].as_deref(), Some("compactor"));
    assert_eq!(rows[0][43].as_deref(), Some("increase_compaction_threads"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("degradation=HighCompactionDebt"));
    assert!(explain.contains("reason_code=RS-3001"));
    assert!(explain.contains("dominant_contributor=compactor"));
    assert!(explain.contains("action=increase_compaction_threads"));
}

#[tokio::test]
async fn test_lag_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "lag_view".to_string(),
        sql: "SELECT a FROM t".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        source_lag_ms: 150,
        compute_lag_ms: 25,
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("lag_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR lag_view;").await;
    assert_eq!(rows[0][7].as_deref(), Some("150"));
    assert_eq!(rows[0][9].as_deref(), Some("25"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("source_lag_ms=150"));
    assert!(explain.contains("compute_lag_ms=25"));
}

#[tokio::test]
async fn test_frontier_facts_reconcile() {
    let _g = TEST_LOCK.lock().await;
    let catalog = CatalogStubs::new();

    catalog.add_view(CatalogView {
        name: "frontier_view".to_string(),
        sql: "SELECT a FROM t".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });

    let facts = ViewEngineFacts {
        frontier: "1050".to_string(),
        ..ViewEngineFacts::default()
    };
    catalog.set_view_engine_facts("frontier_view", facts.clone());

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    let rows = simple_rows(&client, "SHOW VIEW STATUS FOR frontier_view;").await;
    assert_eq!(rows[0][42].as_deref(), Some("1050"));

    let plan = PlanNode::Source {
        name: "t".to_string(),
    };
    let explain = explain_incremental_with_engine_facts(&plan, &[facts]);
    assert!(explain.contains("frontier=1050"));
}
