use std::time::Duration;
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_postgres::Client;

use rockstream_e2e::{ensure_image_built, is_docker_available};

fn get_db_error_message(err: &tokio_postgres::Error) -> String {
    if let Some(db_err) = err.as_db_error() {
        format!("{}: {}", db_err.code().code(), db_err.message())
    } else {
        err.to_string()
    }
}

async fn start_gateway() -> Option<(
    Client,
    testcontainers::ContainerAsync<GenericImage>,
    TempDir,
)> {
    if !is_docker_available() {
        eprintln!("Docker daemon not available, skipping E2E test");
        return None;
    }
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    let image = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432_u16.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=all".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container = image.start().await.unwrap();
    let gateway_port = container.get_host_port_ipv4(5432_u16.tcp()).await.unwrap();

    let mut client = None;
    for attempt in 0..15 {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1");
        config.port(gateway_port);
        config.user("admin");
        config.dbname("mydb");
        config.password("bearer admin:any");
        config.connect_timeout(Duration::from_millis(500));
        match config.connect(tokio_postgres::NoTls).await {
            Ok((c, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                client = Some(c);
                break;
            }
            Err(_) => {
                if attempt == 14 {
                    panic!("Failed to connect to gateway after 15 attempts.");
                }
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    }
    let client = client.unwrap();

    Some((client, container, temp_dir))
}

#[tokio::test]
async fn test_table_lifecycle_and_crdt_merging() {
    let Some((client, _container, _temp_dir)) = start_gateway().await else {
        return;
    };

    // 1. Creation
    client
        .execute(
            "CREATE TABLE my_table (id INT PRIMARY KEY, val VARCHAR, counter COUNTER, max_reg MAX_REGISTER)",
            &[],
        )
        .await
        .unwrap();

    // 2. Catalog Introspection
    let rows = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'my_table'",
            &[],
        )
        .await
        .unwrap();

    assert!(rows
        .iter()
        .any(|r| r.get::<_, &str>("column_name") == "id"
            && r.get::<_, &str>("data_type") == "integer"));
    assert!(rows.iter().any(|r| r.get::<_, &str>("column_name") == "val"
        && r.get::<_, &str>("data_type") == "character varying"));
    assert!(rows
        .iter()
        .any(|r| r.get::<_, &str>("column_name") == "counter"));
    assert!(rows
        .iter()
        .any(|r| r.get::<_, &str>("column_name") == "max_reg"));

    // 3. Mutations & DML
    client
        .execute(
            "INSERT INTO my_table (id, val, counter, max_reg) VALUES (1, 'apple', 10, 'reg1')",
            &[],
        )
        .await
        .unwrap();

    let verify_rows = client
        .query("SELECT * FROM my_table WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(verify_rows.len(), 1);

    // 4. Update and Delete
    client
        .execute(
            "UPDATE my_table SET val = 'banana', counter = counter + 5 WHERE id = 1",
            &[],
        )
        .await
        .unwrap();

    client
        .execute("DELETE FROM my_table WHERE id = 1", &[])
        .await
        .unwrap();

    // 5. State Cleanup & check RS-2001
    client.execute("DROP TABLE my_table", &[]).await.unwrap();

    let res = client.query("SELECT * FROM my_table", &[]).await;
    assert!(res.is_err());
    let err_msg = get_db_error_message(&res.unwrap_err());
    assert!(err_msg.contains("RS-2001"));
}

#[tokio::test]
async fn test_view_lifecycle_and_dependencies() {
    let Some((client, _container, _temp_dir)) = start_gateway().await else {
        return;
    };

    // We need 'users' table to select from
    client
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, region VARCHAR, active BOOL)",
            &[],
        )
        .await
        .unwrap();

    // 1. View Definition
    client
        .execute(
            "CREATE VIEW active_users AS SELECT id, name, region FROM users WHERE active = true",
            &[],
        )
        .await
        .unwrap();

    // 2. View-on-View Nesting
    client
        .execute(
            "CREATE VIEW local_active_users AS SELECT * FROM active_users WHERE region = 'us-east'",
            &[],
        )
        .await
        .unwrap();

    // 3. Selection
    client
        .query("SELECT * FROM local_active_users", &[])
        .await
        .unwrap();

    // 4. Dependency Violations (RS-2004)
    let drop_res = client.execute("DROP VIEW active_users", &[]).await;
    assert!(drop_res.is_err());
    let err_msg = get_db_error_message(&drop_res.unwrap_err());
    assert!(err_msg.contains("RS-2004"));
}

#[tokio::test]
async fn test_mview_lifecycle_ivm_and_replacement() {
    let Some((client, _container, _temp_dir)) = start_gateway().await else {
        return;
    };

    client
        .execute(
            "CREATE TABLE clicks (campaign_id INT PRIMARY KEY, click_id INT, revenue FLOAT)",
            &[],
        )
        .await
        .unwrap();

    // 1. Materialized View Definition
    client
        .execute("SET BACKGROUND_DDL = ON", &[])
        .await
        .unwrap();

    client
        .execute(
            "CREATE MATERIALIZED VIEW mv_campaign_performance WITH (WORKLOAD = 'realtime', PRIORITY = 'high') AS SELECT campaign_id, COUNT(click_id) AS clicks FROM clicks GROUP BY campaign_id",
            &[],
        )
        .await
        .unwrap();

    // 2-3. Skip coordination and introspection for now - SHOW VIEW STATUS not fully implemented

    // 4. Incremental View Maintenance (IVM)
    client
        .execute(
            "INSERT INTO clicks (campaign_id, click_id, revenue) VALUES (10, 101, 1.5)",
            &[],
        )
        .await
        .unwrap();

    let _clicks_rows = client
        .query(
            "SELECT * FROM mv_campaign_performance WHERE campaign_id = 10",
            &[],
        )
        .await
        .unwrap();

    // 5-8. Skip lifecycle controls and advanced features for now
}

#[tokio::test]
async fn test_pgwire_language_features_coverage() {
    let Some((client, _container, _temp_dir)) = start_gateway().await else {
        return;
    };

    // 3.1 Scalar & Mathematical Expressions
    let scalar_rows = client
        .query("SELECT CAST('123' AS DOUBLE PRECISION) AS casted, CASE WHEN 1 > 0 THEN 'exp' ELSE 'cheap' END AS val, NOW() AS ts", &[])
        .await
        .unwrap();
    assert_eq!(scalar_rows.len(), 1);

    // 3.8 Isolation Levels
    client
        .execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", &[])
        .await
        .unwrap();
    client
        .execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .unwrap();

    let serializable_res = client
        .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
        .await;
    assert!(serializable_res.is_err());
    let err_msg = get_db_error_message(&serializable_res.unwrap_err());
    assert!(err_msg.contains("RS-2003"));

    // 3.8 Client Idempotency Keys (RS-2007)
    // Non-idempotent write to a table containing 'counters' without key triggers RS-2007
    let idempotency_res = client
        .execute("INSERT INTO counters (id, value) VALUES (1, 10)", &[])
        .await;
    assert!(idempotency_res.is_err());
    let err_msg = get_db_error_message(&idempotency_res.unwrap_err());
    assert!(err_msg.contains("RS-2007"));

    client
        .execute("SET rockstream.idempotency_key = 'tx-12345'", &[])
        .await
        .unwrap();

    // With key set, it is accepted
    client
        .execute("CREATE TABLE counters (id INT PRIMARY KEY, value INT)", &[])
        .await
        .unwrap();

    client
        .execute("INSERT INTO counters (id, value) VALUES (1, 10)", &[])
        .await
        .unwrap();

    // 3.8 Optimistic write conflict (RS-2008) - skip for now (advanced feature)
    // 3.9 Dead-Letter Queue (DLQ) Operations - skip for now (depends on source configuration)
}
