//! v0.72 producer authority checks: SUBSCRIBE must enforce the same ACL
//! and namespace gate as SELECT before admitting a stream or scanning rows.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pgwire::api::results::Response;
use rockstream_gateway::auth::Principal;
use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};
use rockstream_gateway::server::GatewayHandler;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::GatewayError;
use rockstream_types::acl::{AclEntry, Role};

#[derive(Default)]
struct ObservedReader {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ViewReader for ObservedReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        Some(0)
    }
}

fn catalog_with_view(namespace: &str) -> Arc<CatalogStubs> {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view_in_namespace(CatalogView {
        name: "subscribed".to_owned(),
        sql: "SELECT value FROM source".to_owned(),
        columns: vec![CatalogColumn {
            name: "value".to_owned(),
            data_type: "Utf8".to_owned(),
        }],
        namespace: namespace.to_owned(),
        op_id: None,
    });
    catalog
}

fn attach_viewer(handler: &GatewayHandler) {
    let mut session = handler.sessions.entry("viewer".to_owned()).or_default();
    session.principal = Principal::Jwt {
        sub: "unprivileged".to_owned(),
    };
}

async fn assert_denied(handler: &GatewayHandler, statement: &str, code: &str) {
    let responses = handler
        .dispatch_async_with_conn(statement, Some("viewer"))
        .await
        .unwrap();
    let [Response::Error(error)] = responses.as_slice() else {
        panic!("unauthorized subscription must return one error before streaming");
    };
    assert_eq!(error.severity, "ERROR");
    assert_eq!(error.code, "42501");
    assert!(
        error.message.contains(code),
        "unexpected subscription authorization error: {}",
        error.message
    );
}

#[tokio::test]
async fn unauthorised_snapshots_and_replay_fail_before_reader_or_stream_admission() {
    let reader = Arc::new(ObservedReader::default());
    let handler = GatewayHandler::new(catalog_with_view("public"), reader.clone());
    attach_viewer(&handler);

    for statement in [
        "SUBSCRIBE subscribed AS OF NOW WITH SNAPSHOT",
        "SUBSCRIBE subscribed AS OF EPOCH 5",
    ] {
        assert_denied(&handler, statement, "RS-2401").await;
    }
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.subscribe_registry().active_streams(), 0);
}

#[tokio::test]
async fn viewer_grant_is_required_and_then_allows_only_authorized_subscription() {
    let reader = Arc::new(ObservedReader::default());
    let handler = GatewayHandler::new(catalog_with_view("public"), reader.clone());
    attach_viewer(&handler);
    assert_denied(&handler, "SUBSCRIBE subscribed", "RS-2401").await;

    handler.acl_store.grant(AclEntry {
        principal: "unprivileged".to_owned(),
        namespace: "public".to_owned(),
        view_name: Some("subscribed".to_owned()),
        role: Role::Viewer,
    });
    let response = handler
        .dispatch_async_with_conn("SUBSCRIBE subscribed", Some("viewer"))
        .await
        .unwrap();
    assert!(matches!(response.as_slice(), [Response::Query(_)]));
    assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
    assert_eq!(handler.subscribe_registry().active_streams(), 1);
    drop(response);
    assert_eq!(handler.subscribe_registry().active_streams(), 0);
}

#[tokio::test]
async fn cross_namespace_subscription_is_denied_even_with_viewer_grant() {
    let reader = Arc::new(ObservedReader::default());
    let handler = GatewayHandler::new(catalog_with_view("other_namespace"), reader.clone());
    attach_viewer(&handler);
    handler.acl_store.grant(AclEntry {
        principal: "unprivileged".to_owned(),
        namespace: "public".to_owned(),
        view_name: Some("subscribed".to_owned()),
        role: Role::Viewer,
    });

    assert_denied(
        &handler,
        "SUBSCRIBE subscribed AS OF NOW WITH SNAPSHOT",
        "RS-2402",
    )
    .await;
    assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
    assert_eq!(handler.subscribe_registry().active_streams(), 0);
}
