//! Public PgWire status distinguishes requested revisions from actual local
//! memory enforcement and unverified remote worker acknowledgements.

use std::sync::Arc;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use rockstream_gateway::{
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_types::workload::{MemoryLimit, WorkloadDef};

struct EmptyReader;

#[async_trait::async_trait]
impl ViewReader for EmptyReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn workload_application_status_reports_actual_gateway_limit_and_unknown_worker_revision() {
    let catalog = Arc::new(CatalogStubs::new());
    assert!(catalog.add_workload(
        WorkloadDef::new("status_workload").with_memory_limit(MemoryLimit::new(100))
    ));
    catalog.add_view_in_namespace(CatalogView {
        name: "status_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("status_view", "status_workload");
    catalog.set_view_state_bytes("status_view", 40, None);
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(EmptyReader),
    );
    let (addr, task) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=test dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let read_rows = |messages: Vec<SimpleQueryMessage>| {
        messages
            .into_iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|index| row.get(index).map(str::to_owned))
                        .collect::<Vec<Option<String>>>(),
                ),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        read_rows(
            client
                .simple_query("SHOW WORKLOAD APPLICATION FOR status_workload")
                .await
                .unwrap()
        ),
        vec![vec![
            Some("status_workload".to_string()),
            Some("1".to_string()),
            Some("100".to_string()),
            Some("100".to_string()),
            Some("40".to_string()),
            None,
            Some("worker_ack_unavailable".to_string()),
        ]]
    );

    assert_eq!(
        catalog.update_workload_if_revision(
            WorkloadDef::new("status_workload").with_memory_limit(MemoryLimit::new(60)),
            1,
        ),
        rockstream_gateway::catalog_stubs::WorkloadRevisionOutcome::Updated(2)
    );
    assert_eq!(
        read_rows(
            client
                .simple_query("SHOW WORKLOAD APPLICATION FOR status_workload")
                .await
                .unwrap()
        ),
        vec![vec![
            Some("status_workload".to_string()),
            Some("2".to_string()),
            Some("60".to_string()),
            Some("60".to_string()),
            Some("40".to_string()),
            None,
            Some("worker_ack_unavailable".to_string()),
        ]]
    );
    let missing = client
        .simple_query("SHOW WORKLOAD APPLICATION FOR missing")
        .await
        .unwrap_err();
    assert_eq!(missing.code().map(|code| code.code()), Some("42704"));

    connection_task.abort();
    task.abort();
}
