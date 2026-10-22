//! All operator families use the same assertions at this lane's scale.

#[path = "support/view_refresh_proportionality.rs"]
mod proportionality;

#[tokio::test]
async fn stateless_project_view_refresh_is_o1_at_10k_rows() {
    proportionality::stateless_project(10_000, 1_000).await;
}

#[tokio::test]
async fn aggregate_view_refresh_is_o1_at_10k_rows() {
    proportionality::aggregate(10_000, 1_000).await;
}

#[tokio::test]
async fn join_view_refresh_is_o1_at_10k_rows() {
    proportionality::join(10_000, 1_000).await;
}

#[tokio::test]
async fn session_window_view_refresh_is_o1_at_10k_rows() {
    proportionality::session_window(10_000, 1_000).await;
}
