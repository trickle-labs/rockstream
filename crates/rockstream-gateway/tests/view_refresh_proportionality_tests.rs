//! All operator families use the same assertions at this lane's scale.

#[path = "support/view_refresh_proportionality.rs"]
mod proportionality;

#[tokio::test]
async fn stateless_project_view_refresh_is_o1_at_1m_rows() {
    proportionality::stateless_project(1_000_000, 50_000).await;
}

#[tokio::test]
async fn aggregate_view_refresh_is_o1_at_1m_rows() {
    proportionality::aggregate(1_000_000, 50_000).await;
}

#[tokio::test]
async fn join_view_refresh_is_o1_at_1m_rows() {
    proportionality::join(1_000_000, 50_000).await;
}

#[tokio::test]
async fn session_window_view_refresh_is_o1_at_1m_rows() {
    proportionality::session_window(1_000_000, 50_000).await;
}
