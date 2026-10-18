//! Controlled HTTP boundary for KMS protocol tests; this is not cloud qualification.

use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
}

pub struct KmsFixture {
    pub endpoint: String,
    pub requests: Arc<Mutex<Vec<Request>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for KmsFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl KmsFixture {
    pub async fn start(handler: impl Fn(&Request) -> (u16, Value) + Send + Sync + 'static) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0, "incomplete HTTP headers");
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let text = std::str::from_utf8(&bytes[..header_end]).unwrap();
                let mut lines = text.lines();
                let method = lines.next().unwrap().to_string();
                let headers: BTreeMap<_, _> = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
                    .collect();
                let length: usize = headers["content-length"].parse().unwrap();
                while bytes.len() < header_end + length {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0, "incomplete HTTP body");
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let request = Request {
                    method,
                    headers,
                    body: serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap(),
                };
                let (status, response) = handler(&request);
                recorded.lock().unwrap().push(request);
                let body = response.to_string();
                let location = if status == 307 {
                    "Location: http://127.0.0.1:9/\r\n"
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: {}\r\n{location}Connection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }
}
