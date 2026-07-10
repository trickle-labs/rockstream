use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::oneshot;

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
