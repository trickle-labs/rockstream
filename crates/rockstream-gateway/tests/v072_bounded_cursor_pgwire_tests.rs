//! Normal-client cursor admission must not bypass producer budgets or SELECT ACL.
use std::sync::Arc;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

struct CursorReader;

#[async_trait::async_trait]
impl ViewReader for CursorReader {
    async fn read_view(
        &self,
        name: &str,
        limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        // The producer must always receive a real row limit, not None.
        assert_eq!(
            limit,
            Some(rockstream_gateway::copy_out_budget::MAX_COPY_OUT_ROWS + 1)
        );
        Ok(match name {
            "big_view" => vec![b"x".to_vec(); 10_001],
            "medium_view" => vec![vec![b'a'; 5 * 1024 * 1024]],
            "small_view" => vec![b"first".to_vec(), b"second".to_vec()],
            _ => Vec::new(),
        })
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn eager_cursors_reject_unbounded_and_stacked_work_without_retaining_rows() {
    let catalog = Arc::new(CatalogStubs::new());
    for name in ["big_view", "medium_view", "small_view"] {
        catalog.add_view(CatalogView {
            name: name.to_string(),
            sql: "SELECT 'x'".to_string(),
            columns: vec![CatalogColumn {
                name: "value".to_string(),
                data_type: "Utf8".to_string(),
            }],
            namespace: "public".to_string(),
            op_id: None,
        });
    }
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(CursorReader),
    );
    let (address, task) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let too_many = client
        .simple_query("DECLARE huge CURSOR FOR SELECT * FROM big_view")
        .await
        .unwrap_err();
    assert_eq!(too_many.code().map(|code| code.code()), Some("54000"));
    let absent = client
        .simple_query("FETCH ALL FROM huge")
        .await
        .unwrap_err();
    assert_eq!(absent.code().map(|code| code.code()), Some("34000"));

    let stacked = client
        .simple_query("DECLARE injection CURSOR FOR DELETE FROM small_view")
        .await
        .unwrap_err();
    assert_eq!(stacked.code().map(|code| code.code()), Some("0A000"));

    client
        .simple_query("DECLARE active CURSOR FOR SELECT * FROM medium_view")
        .await
        .unwrap();
    let budget = client
        .simple_query("DECLARE second CURSOR FOR SELECT * FROM medium_view")
        .await
        .unwrap_err();
    assert_eq!(budget.code().map(|code| code.code()), Some("54000"));
    client.simple_query("CLOSE active").await.unwrap();

    client
        .simple_query("DECLARE ok CURSOR FOR SELECT * FROM small_view")
        .await
        .unwrap();
    let rows: Vec<_> = client
        .simple_query("FETCH ALL FROM ok")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(rows, vec!["first".to_string(), "second".to_string()]);
    client.simple_query("CLOSE ok").await.unwrap();

    // Cursor page order must survive pipelined normal-client reads.
    client
        .simple_query("DECLARE pipelined CURSOR FOR SELECT * FROM small_view")
        .await
        .unwrap();
    let (page_one, page_two) = tokio::join!(
        client.simple_query("FETCH 1 FROM pipelined"),
        client.simple_query("FETCH 1 FROM pipelined"),
    );
    assert!(page_one
        .unwrap()
        .iter()
        .any(|row| matches!(row, SimpleQueryMessage::Row(r) if r.get(0) == Some("first"))));
    assert!(page_two
        .unwrap()
        .iter()
        .any(|row| matches!(row, SimpleQueryMessage::Row(r) if r.get(0) == Some("second"))));
    client.simple_query("CLOSE pipelined").await.unwrap();

    // A large explicit count must clamp to the remaining rows, even after
    // the cursor position is nonzero (usize arithmetic must not overflow).
    client
        .simple_query("DECLARE large_fetch CURSOR FOR SELECT * FROM small_view")
        .await
        .unwrap();
    client
        .simple_query("FETCH 1 FROM large_fetch")
        .await
        .unwrap();
    let remaining: Vec<_> = client
        .simple_query(&format!("FETCH {} FROM large_fetch", usize::MAX))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();
    assert_eq!(remaining, vec!["second".to_string()]);
    client.simple_query("CLOSE large_fetch").await.unwrap();

    client
        .simple_query("DECLARE large_move CURSOR FOR SELECT * FROM small_view")
        .await
        .unwrap();
    client.simple_query("MOVE 1 FROM large_move").await.unwrap();
    client
        .simple_query(&format!("MOVE {} FROM large_move", usize::MAX))
        .await
        .unwrap();
    let after_move = client
        .simple_query("FETCH ALL FROM large_move")
        .await
        .unwrap();
    assert!(!after_move
        .iter()
        .any(|message| matches!(message, SimpleQueryMessage::Row(_))));
    client.simple_query("CLOSE large_move").await.unwrap();

    connection_task.abort();
    task.abort();
}

struct DeniedReader;

#[async_trait::async_trait]
impl ViewReader for DeniedReader {
    async fn read_view(
        &self,
        _name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        panic!("unauthorized cursor must not invoke a producer");
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn unauthorized_cursor_is_rejected_before_producer_admission() {
    use pgwire::api::results::Response;
    use rockstream_gateway::auth::Principal;
    use rockstream_gateway::server::GatewayHandler;

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "private_cursor_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![CatalogColumn {
            name: "value".to_string(),
            data_type: "Utf8".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });
    let handler = GatewayHandler::new(catalog, Arc::new(DeniedReader));
    {
        let mut session = handler.sessions.entry("viewer".to_string()).or_default();
        session.principal = Principal::Jwt {
            sub: "unprivileged".to_string(),
        };
    }

    let responses = handler
        .dispatch_async_with_conn(
            "DECLARE denied CURSOR FOR SELECT * FROM private_cursor_view",
            Some("viewer"),
        )
        .await
        .unwrap();
    let [Response::Error(error)] = responses.as_slice() else {
        panic!("cursor must fail before producer read");
    };
    assert_eq!(error.code, "42501");
}

// One gateway admits at most four concurrent eager producers, regardless of
// connection identity. Dropping a read releases the RAII permit.
struct SlowCursorReader;

#[async_trait::async_trait]
impl ViewReader for SlowCursorReader {
    async fn read_view(
        &self,
        _name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        Ok(vec![b"ok".to_vec()])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn cursor_producers_have_shared_fail_fast_admission_and_release() {
    use pgwire::api::results::Response;
    use rockstream_gateway::server::GatewayHandler;

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "small_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![CatalogColumn {
            name: "value".to_string(),
            data_type: "Utf8".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });
    let handler = GatewayHandler::new(catalog, Arc::new(SlowCursorReader));
    let sql = "DECLARE active CURSOR FOR SELECT * FROM small_view";
    let (a, b, c, d, e) = tokio::join!(
        handler.dispatch_async_with_conn(sql, Some("conn_a")),
        handler.dispatch_async_with_conn(sql, Some("conn_b")),
        handler.dispatch_async_with_conn(sql, Some("conn_c")),
        handler.dispatch_async_with_conn(sql, Some("conn_d")),
        handler.dispatch_async_with_conn(sql, Some("conn_e")),
    );
    let results = [a.unwrap(), b.unwrap(), c.unwrap(), d.unwrap(), e.unwrap()];
    let rejected = results
        .iter()
        .filter(|responses| {
            matches!(responses.as_slice(), [Response::Error(error)] if error.code == "54000" && error.message.contains("cursor.producer_busy"))
        })
        .count();
    assert_eq!(
        rejected, 1,
        "exactly the fifth concurrent producer must be rejected"
    );
    let retried = handler
        .dispatch_async_with_conn(sql, Some("conn_f"))
        .await
        .unwrap();
    assert!(
        retried
            .iter()
            .all(|response| !matches!(response, Response::Error(_))),
        "permit must be released after producer completes"
    );
}

#[tokio::test]
async fn cancelled_parallel_cursor_reads_release_producer_slots() {
    use pgwire::api::results::Response;
    use rockstream_gateway::server::GatewayHandler;

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "small_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![CatalogColumn {
            name: "value".to_string(),
            data_type: "Utf8".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });
    let handler = GatewayHandler::new(catalog, Arc::new(SlowCursorReader));
    let sql = "DECLARE active CURSOR FOR SELECT * FROM small_view";
    let blocked = {
        let reads = async {
            tokio::join!(
                handler.dispatch_async_with_conn(sql, Some("one")),
                handler.dispatch_async_with_conn(sql, Some("two")),
                handler.dispatch_async_with_conn(sql, Some("three")),
                handler.dispatch_async_with_conn(sql, Some("four")),
            )
        };
        tokio::pin!(reads);
        tokio::select! {
            _ = &mut reads => panic!("the producer test fixture must remain blocked"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                handler.dispatch_async_with_conn(sql, Some("five")).await.unwrap()
            }
        }
    };
    assert!(matches!(
        blocked.as_slice(),
        [Response::Error(error)] if error.code == "54000"
            && error.message.contains("cursor.producer_busy")
    ));
    // The four in-flight futures were dropped by leaving the scope.
    // Their owned semaphore permits must no longer count against admission.
    let retried = handler
        .dispatch_async_with_conn(sql, Some("six"))
        .await
        .unwrap();
    assert!(retried
        .iter()
        .all(|response| !matches!(response, Response::Error(_))));
}
