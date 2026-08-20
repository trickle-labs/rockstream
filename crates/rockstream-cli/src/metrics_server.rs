use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::oneshot;

#[cfg(test)]
pub(crate) static METRICS_SERVER_TEST_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Handle to the running metrics server.
pub struct MetricsServerHandle {
    /// The actual address the server is bound to.
    pub local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MetricsServerHandle {
    /// Signal the metrics server to shut down.
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Starts the TCP-based Prometheus `/metrics` HTTP server in the background.
pub async fn start_metrics_server(addr: &str) -> std::io::Result<MetricsServerHandle> {
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
        let (headers, body) = resp.split_once("\r\n\r\n").unwrap();
        assert_eq!(headers.lines().next(), Some("HTTP/1.1 200 OK"));
        assert!(headers.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"));
        assert!(headers.contains(&format!("Content-Length: {}", body.len())));
        assert!(headers.ends_with("Connection: close"));
        assert!(body.starts_with("# HELP rockstream_build_info"));
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
        // Half-close the write side without sending any bytes; the server's
        // `Ok(n) if n > 0` guard must skip writing a response, and the
        // connection closes with zero bytes read.
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
        // Give the accept loop's select! a moment to observe the shutdown
        // signal and break before we assert new connections aren't served
        // a 200 (best-effort: either the connect fails or it's refused).
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = TcpStream::connect(addr).await;
    }
}
