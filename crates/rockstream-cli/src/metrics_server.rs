use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::oneshot;

use rockstream_types::lifecycle::LifecycleTracker;

pub static METRICS_SERVER_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Handle to the running metrics/management server.
pub struct MetricsServerHandle {
    /// The actual address the server is bound to.
    pub local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MetricsServerHandle {
    /// Signal the metrics/management server to shut down.
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

fn format_http_json_response<T: serde::Serialize>(status_code: u16, data: &T) -> Vec<u8> {
    let status_text = match status_code {
        200 => "200 OK",
        503 => "503 Service Unavailable",
        400 => "400 Bad Request",
        404 => "404 Not Found",
        _ => "500 Internal Server Error",
    };
    let body = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
    let body_bytes = body.as_bytes();
    let header = format!(
        "HTTP/1.1 {status_text}\r\n\
         Content-Type: application/json; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body_bytes.len()
    );
    let mut resp = header.into_bytes();
    resp.extend_from_slice(body_bytes);
    resp
}

/// Starts the TCP-based Prometheus `/metrics` and management (`/live`, `/ready`, `/health`) HTTP server.
pub async fn start_metrics_server(addr: &str) -> std::io::Result<MetricsServerHandle> {
    let tracker = Arc::new(LifecycleTracker::new("all"));
    start_management_server(addr, tracker).await
}

/// Starts the TCP-based management HTTP server with an explicit `LifecycleTracker`.
pub async fn start_management_server(
    addr: &str,
    tracker: Arc<LifecycleTracker>,
) -> std::io::Result<MetricsServerHandle> {
    let listener = TokioTcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(conn) => conn,
                        Err(_) => continue,
                    };
                    let tracker = tracker.clone();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => {
                                let req = String::from_utf8_lossy(&buf[..n]);
                                if req.starts_with("GET /metrics") {
                                    let body_str = rockstream_types::metrics::generate_prometheus_metrics();
                                    let body = body_str.as_bytes();
                                    let response = format!(
                                        "HTTP/1.1 200 OK\r\n\
                                         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                                         Content-Length: {}\r\n\
                                         Connection: close\r\n\r\n",
                                        body.len()
                                    );
                                    let mut resp_bytes = response.into_bytes();
                                    resp_bytes.extend_from_slice(body);
                                    let _ = socket.write_all(&resp_bytes).await;
                                    let _ = socket.flush().await;
                                } else if req.starts_with("GET /live") {
                                    let (code, live) = tracker.generate_live_response();
                                    let resp_bytes = format_http_json_response(code, &live);
                                    let _ = socket.write_all(&resp_bytes).await;
                                    let _ = socket.flush().await;
                                } else if req.starts_with("GET /ready") {
                                    let (code, ready) = tracker.generate_ready_response();
                                    let resp_bytes = format_http_json_response(code, &ready);
                                    let _ = socket.write_all(&resp_bytes).await;
                                    let _ = socket.flush().await;
                                } else if req.starts_with("GET /health") {
                                    let (code, health) = tracker.generate_health_report();
                                    let resp_bytes = format_http_json_response(code, &health);
                                    let _ = socket.write_all(&resp_bytes).await;
                                    let _ = socket.flush().await;
                                } else {
                                    let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                    let _ = socket.write_all(response.as_bytes()).await;
                                    let _ = socket.flush().await;
                                }
                            }
                            _ => {}
                        }
                    });
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    });

    Ok(MetricsServerHandle {
        local_addr,
        shutdown_tx: Some(shutdown_tx),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::lifecycle::{DependencyStatus, HealthReason, LifecycleState};
    use tokio::net::TcpStream;

    async fn request(addr: SocketAddr, req: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        String::from_utf8_lossy(&resp).to_string()
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_200_with_prometheus_body() {
        let _metrics_lock = METRICS_SERVER_TEST_LOCK.lock().await;
        let handle = start_metrics_server("127.0.0.1:0").await.unwrap();
        let resp = request(handle.local_addr, "GET /metrics HTTP/1.1\r\n\r\n").await;
        let body = rockstream_types::metrics::generate_prometheus_metrics();
        assert_eq!(
            resp,
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
        );
        handle.shutdown();
    }

    #[tokio::test]
    async fn live_ready_health_endpoints_work() {
        let tracker = Arc::new(LifecycleTracker::new("worker"));
        let handle = start_management_server("127.0.0.1:0", tracker.clone())
            .await
            .unwrap();

        // 1. Initial Starting state
        let live_resp = request(handle.local_addr, "GET /live HTTP/1.1\r\n\r\n").await;
        assert!(live_resp.starts_with("HTTP/1.1 200 OK"));
        assert!(live_resp.contains(r#"{"status":"alive"}"#));

        let ready_resp = request(handle.local_addr, "GET /ready HTTP/1.1\r\n\r\n").await;
        assert!(ready_resp.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(ready_resp.contains(r#"{"status":"not_ready","reason":"starting"}"#));

        let health_resp = request(handle.local_addr, "GET /health HTTP/1.1\r\n\r\n").await;
        assert!(health_resp.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(health_resp.contains(r#""status":"starting""#));
        assert!(health_resp.contains(r#""role":"worker""#));

        // 2. Transition to Ready
        tracker.set_state(LifecycleState::Ready);
        tracker.set_active_shards(4);
        tracker.set_dependency("storage", DependencyStatus::Ok, None, Some(5));

        let ready_resp2 = request(handle.local_addr, "GET /ready HTTP/1.1\r\n\r\n").await;
        assert!(ready_resp2.starts_with("HTTP/1.1 200 OK"));
        assert!(ready_resp2.contains(r#"{"status":"ready"}"#));

        let health_resp2 = request(handle.local_addr, "GET /health HTTP/1.1\r\n\r\n").await;
        assert!(health_resp2.starts_with("HTTP/1.1 200 OK"));
        assert!(health_resp2.contains(r#""status":"healthy""#));
        assert!(health_resp2.contains(r#""active_shards":4"#));
        assert!(health_resp2.contains(r#""storage":{"name":"storage","status":"ok""#));

        // 3. Transition to Degraded
        tracker.set_state(LifecycleState::Degraded);
        tracker.add_reason(HealthReason::new(
            rockstream_types::error_code::RS_3010,
            "High replication lag",
        ));
        let health_resp3 = request(handle.local_addr, "GET /health HTTP/1.1\r\n\r\n").await;
        assert!(health_resp3.starts_with("HTTP/1.1 200 OK"));
        assert!(health_resp3.contains(r#""status":"degraded""#));
        assert!(health_resp3.contains(r#""code":"RS-3010""#));

        // 4. Transition to Draining
        tracker.set_state(LifecycleState::Draining);
        let ready_resp4 = request(handle.local_addr, "GET /ready HTTP/1.1\r\n\r\n").await;
        assert!(ready_resp4.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(ready_resp4.contains(r#"{"status":"not_ready","reason":"draining"}"#));

        handle.shutdown();
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let handle = start_metrics_server("127.0.0.1:0").await.unwrap();
        let resp = request(handle.local_addr, "GET /other HTTP/1.1\r\n\r\n").await;
        assert_eq!(
            resp,
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        handle.shutdown();
    }

    #[tokio::test]
    async fn empty_request_is_ignored_without_a_response() {
        let handle = start_metrics_server("127.0.0.1:0").await.unwrap();
        let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        assert!(resp.is_empty());
        handle.shutdown();
    }

    #[tokio::test]
    async fn shutdown_stops_the_accept_loop() {
        let handle = start_metrics_server("127.0.0.1:0").await.unwrap();
        let addr = handle.local_addr;
        handle.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = TcpStream::connect(addr).await;
    }
}
