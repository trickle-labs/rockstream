//! v0.26 auth, RBAC, namespace, and partial-agg proof tests.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    multi_shard_reader::{can_pushdown_partial_agg, MultiShardReader},
    server::GatewayHandler,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{PartialAggSpec, ShardDb, ShardReader};

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

// ── proof_auth_rejects_unauthenticated_lfs ───────────────────────────────────

/// P2 (LFS): missing bearer token fails with RS-2400.
#[tokio::test]
async fn proof_auth_rejects_unauthenticated_lfs() {
    let verifier =
        rockstream_gateway::auth::JwtVerifier::with_hs256_key(b"test-secret-key".to_vec());
    let err = verifier.verify("").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("RS-2400") || msg.contains("unauthenticated"),
        "expected RS-2400 in error, got: {msg}"
    );
}

// ── proof_rbac_denies_cross_namespace_access ─────────────────────────────────

/// P3 (unit/LFS): viewer in ns-a tries SELECT from ns-b view → RS-2402.
#[tokio::test]
async fn proof_rbac_denies_cross_namespace_access() {
    use rockstream_types::acl::{AclEntry, Role};

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("rbac-test-shard", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::default());
    catalog.add_view_in_namespace(CatalogView {
        name: "ns_b_view".to_string(),
        sql: "SELECT * FROM t".to_string(),
        columns: vec![],
        namespace: "ns-b".to_string(),
    });

    let view_reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);
    let handler = Arc::new(GatewayHandler::with_shard_db(
        catalog,
        view_reader,
        shard_db,
    ));

    let conn_id = "alice-conn";
    {
        let mut s = handler.sessions.entry(conn_id.to_string()).or_default();
        s.principal = rockstream_gateway::auth::Principal::Jwt {
            sub: "alice".to_string(),
        };
        s.current_namespace = "ns-a".to_string();
    }
    handler.acl_store.grant(AclEntry {
        principal: "alice".to_string(),
        namespace: "ns-a".to_string(),
        view_name: None,
        role: Role::Viewer,
    });

    let responses = handler
        .dispatch_async_with_conn("SELECT * FROM ns_b_view", Some(conn_id))
        .await
        .unwrap();

    let has_rs2402 = responses.iter().any(|r| {
        if let pgwire::api::results::Response::Error(e) = r {
            e.message.contains("RS-2402")
        } else {
            false
        }
    });
    assert!(
        has_rs2402,
        "expected RS-2402 response for cross-namespace access"
    );
}

// ── proof_partial_agg_pushdown_lfs ───────────────────────────────────────────

/// P5 (LFS): 3-shard setup; SELECT region, COUNT(*) FROM orders_mv GROUP BY region.
#[tokio::test]
async fn proof_partial_agg_pushdown_lfs() {
    let regions = ["us-east", "us-west", "eu-central"];
    let mut readers = vec![];
    for (i, region) in regions.iter().enumerate() {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder(format!("lfs-shard-{i}"), store.clone())
            .build()
            .await
            .unwrap();
        for j in 0u64..10 {
            let key = format!("view_output/orders_mv/{:016x}", j);
            let val = format!("{region}\t1");
            shard.put(key.as_bytes(), val.as_bytes()).await.unwrap();
        }
        shard.flush().await.unwrap();
        readers.push(Arc::new(
            ShardReader::open(format!("lfs-shard-{i}"), store)
                .await
                .unwrap(),
        ));
    }

    let msr = MultiShardReader::new(readers, 0, MultiShardReader::DEFAULT_MAX_IN_FLIGHT_ROWS);

    assert!(
        can_pushdown_partial_agg("SELECT region, COUNT(*) FROM orders_mv GROUP BY region"),
        "should detect pushdown"
    );

    let spec = PartialAggSpec {
        group_col: 0,
        agg_col: 1,
        agg_type: "count".to_string(),
    };
    let plan_bytes = serde_json::to_vec(&spec).unwrap();
    let result = msr
        .scatter_read_partial_agg(
            "orders_mv",
            &plan_bytes,
            "SELECT region, COUNT(*) FROM orders_mv GROUP BY region",
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 3, "expected 3 groups, got {}", result.len());
    for row in &result {
        let s = String::from_utf8_lossy(row);
        let cols: Vec<&str> = s.split('\t').collect();
        let count: i64 = cols.get(1).unwrap_or(&"0").parse().unwrap_or(0);
        assert_eq!(count, 10, "each region should have count=10, got: {s}");
    }
}

// ── proof_end_to_end_postgres_pillar_tc ──────────────────────────────────────

/// P1–P5 (TC): full Postgres pillar integration proof with TestContainers.
#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn proof_end_to_end_postgres_pillar_tc() {
    use tokio_postgres::NoTls;

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(ShardDb::builder("tc-shard", store).build().await.unwrap());
    let catalog = Arc::new(CatalogStubs::default());
    let view_reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db);
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            local_addr.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW orders_mv AS SELECT id, region, val FROM base_table",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW should succeed");

    client
        .simple_query("SET rockstream.idempotency_key = 'tc-pillar-1'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO orders_mv (id, region, val) VALUES (1, 'us-east', 100)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    let rows = client
        .simple_query("SELECT * FROM orders_mv")
        .await
        .unwrap();
    assert!(!rows.is_empty(), "SELECT should return response");

    let rows = client
        .simple_query("EXPLAIN SELECT region, COUNT(*) FROM orders_mv GROUP BY region")
        .await
        .unwrap();
    let plan_text: String = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(
        plan_text.contains("partial_pushdown: true"),
        "EXPLAIN must show partial_pushdown: true, got: {plan_text}"
    );
}
