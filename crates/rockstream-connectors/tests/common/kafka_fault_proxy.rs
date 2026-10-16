//! Kafka wire faults against a real broker, scoped to EndTxn and lookup replies.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Image, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct Faults {
    /// 0: pass, 1: lose EndTxn acknowledgement, 2: interrupt before EndTxn.
    pub commit: AtomicU8,
    pub lookup_unavailable: AtomicBool,
    pub interrupted: Notify,
    pub commit_response: Mutex<Option<i16>>,
}

pub struct KafkaFaultProxy {
    pub bootstrap: String,
    pub faults: Arc<Faults>,
    _broker: ContainerAsync<GenericImage>,
    task: JoinHandle<()>,
}

impl Drop for KafkaFaultProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl KafkaFaultProxy {
    pub async fn start() -> Self {
        assert!(
            crate::common::docker_available(),
            "Docker is required for Kafka faults"
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bootstrap = listener.local_addr().unwrap().to_string();
        let kafka = apache::Kafka::default();
        let mut image = GenericImage::new(kafka.name(), kafka.tag())
            .with_exposed_port(KAFKA_PORT)
            .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        // Keep the standard fixture settings; advertise the proxy for every
        // producer/consumer connection, including metadata-discovered brokers.
        for (key, value) in kafka.env_vars() {
            image = image.with_env_var(key.into().into_owned(), value.into().into_owned());
        }
        image = image
            .with_env_var(
                "KAFKA_ADVERTISED_LISTENERS",
                format!("PLAINTEXT://{bootstrap},BROKER://localhost:9093"),
            )
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        let broker = image.start().await.unwrap();
        let upstream = format!(
            "127.0.0.1:{}",
            broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
        );
        let faults = Arc::new(Faults::default());
        let state = faults.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (client, _) = connection.unwrap();
                        let upstream = upstream.clone();
                        let state = state.clone();
                        connections.spawn(async move {
                            let server = TcpStream::connect(upstream).await?;
                            relay(client, server, state).await
                        });
                    }
                    Some(result) = connections.join_next() => {
                        // Clients close connections during the fault and restart.
                        if let Err(error) = result {
                            panic!("Kafka proxy task failed: {error}");
                        }
                    }
                }
            }
        });
        Self {
            bootstrap,
            faults,
            _broker: broker,
            task,
        }
    }
}

async fn frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let size = reader.read_i32().await?;
    if !(4..=16 * 1024 * 1024).contains(&size) {
        return Err(io::Error::other("invalid Kafka frame size"));
    }
    let mut bytes = vec![0; size as usize];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

async fn send(writer: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> io::Result<()> {
    writer.write_i32(bytes.len() as i32).await?;
    writer.write_all(bytes).await
}

async fn relay(client: TcpStream, server: TcpStream, faults: Arc<Faults>) -> io::Result<()> {
    let (mut client_read, mut client_write) = client.into_split();
    let (mut server_read, mut server_write) = server.into_split();
    let requests = Mutex::new(BTreeMap::new());
    tokio::try_join!(
        async {
            loop {
                let request = frame(&mut client_read).await?;
                assert!(request.len() >= 10, "Kafka request header");
                let api = i16::from_be_bytes(request[0..2].try_into().unwrap());
                let version = i16::from_be_bytes(request[2..4].try_into().unwrap());
                let correlation = i32::from_be_bytes(request[4..8].try_into().unwrap());
                let client_length = i16::from_be_bytes(request[8..10].try_into().unwrap());
                let lookup = client_length >= 0
                    && request.get(10..10 + client_length as usize)
                        == Some(b"rockstream-epoch-check".as_slice());
                if api == 26 && faults.commit.load(Ordering::SeqCst) == 2 {
                    faults.interrupted.notify_one();
                    continue;
                }
                requests
                    .lock()
                    .unwrap()
                    .insert(correlation, (api, version, lookup));
                send(&mut server_write, &request).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        },
        async {
            loop {
                let response = frame(&mut server_read).await?;
                let correlation = i32::from_be_bytes(response[0..4].try_into().unwrap());
                let (api, version, lookup) = requests
                    .lock()
                    .unwrap()
                    .remove(&correlation)
                    .expect("known Kafka correlation");
                if api == 26 && faults.commit.load(Ordering::SeqCst) == 1 {
                    // EndTxn v3+ adds an empty flexible response-header tag buffer.
                    let error_offset = if version >= 3 { 9 } else { 8 };
                    if version >= 3 {
                        assert_eq!(response[4], 0, "expected no response header tags");
                    }
                    let error = i16::from_be_bytes(
                        response[error_offset..error_offset + 2].try_into().unwrap(),
                    );
                    *faults.commit_response.lock().unwrap() = Some(error);
                    faults.interrupted.notify_one();
                    continue;
                }
                // ListOffsets and Fetch are consumer lookup operations. Producer
                // initialization, Produce, and EndTxn remain reachable.
                if lookup
                    && matches!(api, 1 | 2)
                    && faults.lookup_unavailable.load(Ordering::SeqCst)
                {
                    continue;
                }
                send(&mut client_write, &response).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), io::Error>(())
        }
    )?;
    Ok(())
}
