//! gRPC external connector client and server stubs for RockStream (v0.49).

use crate::source::{LifecycleSource, SchemaAwareSource, Source};
use async_trait::async_trait;
use rockstream_types::batch::{OffsetToken, SourceBatch};
use rockstream_types::connector::{
    ConnectorLifecycleState, LawSchemaMetadata, WriteClassification,
};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// ─── protobuf message representations ────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PollBatchRequest {
    pub epoch: u64,
}

#[derive(Debug, Clone)]
pub struct PollBatchResponse {
    pub record_count: u64,
    pub epoch: u64,
    pub offset: String,
    pub watermark: u64,
}

#[derive(Debug, Clone)]
pub struct DiscoverSchemaRequest;

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub law_id: u32,
    pub crdt_type: String,
    pub write_classification: String,
}

#[derive(Debug, Clone)]
pub struct DiscoverSchemaResponse {
    pub columns: BTreeMap<String, ColumnMetadata>,
}

#[derive(Debug, Clone)]
pub struct LifecycleRequest {
    pub action: String,
}

#[derive(Debug, Clone)]
pub struct LifecycleResponse {
    pub success: bool,
    pub state: String,
}

// ─── gRPC Service Interface ──────────────────────────────────────────────────

/// Tonic-like gRPC service trait for external connectors.
#[async_trait]
pub trait ConnectorService: Send + Sync {
    async fn poll_batch(&self, request: PollBatchRequest) -> Result<PollBatchResponse, String>;
    async fn discover_schema(
        &self,
        request: DiscoverSchemaRequest,
    ) -> Result<DiscoverSchemaResponse, String>;
    async fn lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleResponse, String>;
}

// ─── gRPC client implementation ──────────────────────────────────────────────

/// A simulated gRPC connector client implementing the `Source` trait.
pub struct GrpcConnectorClient {
    name: String,
    service: Arc<dyn ConnectorService>,
    credits: usize,
    current_offset: Mutex<Option<OffsetToken>>,
}

impl GrpcConnectorClient {
    pub fn new(name: impl Into<String>, service: Arc<dyn ConnectorService>) -> Self {
        Self {
            name: name.into(),
            service,
            credits: usize::MAX,
            current_offset: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Source for GrpcConnectorClient {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if Source::lifecycle_state(self) != ConnectorLifecycleState::Running {
            let offset_val = self.current_offset.lock().unwrap().clone();
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: offset_val,
                watermark: Some(epoch * 100),
            });
        }

        if self.credits == 0 {
            let offset_val = self.current_offset.lock().unwrap().clone();
            return Some(SourceBatch {
                record_count: 0,
                epoch,
                offset: offset_val,
                watermark: Some(epoch * 100),
            });
        }

        let req = PollBatchRequest { epoch };
        if let Ok(res) = self.service.poll_batch(req).await {
            let offset = OffsetToken(res.offset);
            *self.current_offset.lock().unwrap() = Some(offset.clone());
            Some(SourceBatch {
                record_count: res.record_count as usize,
                epoch: res.epoch,
                offset: Some(offset),
                watermark: Some(res.watermark),
            })
        } else {
            None
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn credits_available(&self) -> usize {
        self.credits
    }

    fn set_credits(&mut self, credits: usize) {
        self.credits = credits;
    }

    fn current_offset(&self) -> Option<OffsetToken> {
        self.current_offset.lock().unwrap().clone()
    }

    fn discover_schema(&self) -> LawSchemaMetadata {
        let req = DiscoverSchemaRequest;
        let mut meta = LawSchemaMetadata::empty();
        if let Ok(res) = block_on(self.service.discover_schema(req)) {
            for (col_name, col_meta) in res.columns {
                let classification = match col_meta.write_classification.as_str() {
                    "blind_delta" => WriteClassification::BlindDelta,
                    "read_dependent_delta" => WriteClassification::ReadDependentDelta,
                    "exact_key_guarded_delta" => WriteClassification::ExactKeyGuardedDelta,
                    "source_exactly_once_protected" => {
                        WriteClassification::SourceExactlyOnceProtected
                    }
                    _ => WriteClassification::BlindDelta,
                };
                meta = meta.with_column(
                    col_name,
                    MergeLawId(col_meta.law_id as u16),
                    col_meta.crdt_type,
                    classification,
                );
            }
        }
        meta
    }

    fn lifecycle_state(&self) -> ConnectorLifecycleState {
        let req = LifecycleRequest {
            action: "get_state".to_string(),
        };
        if let Ok(res) = block_on(self.service.lifecycle(req)) {
            match res.state.as_str() {
                "paused" => ConnectorLifecycleState::Paused,
                "deleted" => ConnectorLifecycleState::Deleted,
                _ => ConnectorLifecycleState::Running,
            }
        } else {
            ConnectorLifecycleState::Running
        }
    }

    async fn pause(&mut self) -> bool {
        let req = LifecycleRequest {
            action: "pause".to_string(),
        };
        if let Ok(res) = self.service.lifecycle(req).await {
            res.success
        } else {
            false
        }
    }

    async fn resume(&mut self) -> bool {
        let req = LifecycleRequest {
            action: "resume".to_string(),
        };
        if let Ok(res) = self.service.lifecycle(req).await {
            res.success
        } else {
            false
        }
    }

    async fn delete(&mut self) {
        let req = LifecycleRequest {
            action: "delete".to_string(),
        };
        self.service.lifecycle(req).await.ok();
    }
}

impl SchemaAwareSource for GrpcConnectorClient {}
impl LifecycleSource for GrpcConnectorClient {}

fn block_on<F: std::future::Future>(mut future: F) -> F::Output {
    let raw_waker = std::task::RawWaker::new(std::ptr::null(), &VTABLE);
    let waker = unsafe { std::task::Waker::from_raw(raw_waker) };
    let mut cx = std::task::Context::from_waker(&waker);
    let mut future = unsafe { std::pin::Pin::new_unchecked(&mut future) };
    loop {
        match future.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(res) => return res,
            std::task::Poll::Pending => {}
        }
    }
}

static VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(
    |_| std::task::RawWaker::new(std::ptr::null(), &VTABLE),
    |_| {},
    |_| {},
    |_| {},
);

// ─── Reference Server Implementation ─────────────────────────────────────────

/// A reference connector service simulating the gRPC server stub.
pub struct ReferenceConnectorService {
    state: Mutex<ConnectorLifecycleState>,
    offset: Mutex<u64>,
}

impl ReferenceConnectorService {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(ConnectorLifecycleState::Running),
            offset: Mutex::new(0),
        }
    }
}

impl Default for ReferenceConnectorService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConnectorService for ReferenceConnectorService {
    async fn poll_batch(&self, request: PollBatchRequest) -> Result<PollBatchResponse, String> {
        let state = *self.state.lock().unwrap();
        if state != ConnectorLifecycleState::Running {
            let offset_val = *self.offset.lock().unwrap();
            return Ok(PollBatchResponse {
                record_count: 0,
                epoch: request.epoch,
                offset: format!("grpc-offset-{offset_val}"),
                watermark: request.epoch * 100,
            });
        }

        let mut offset_guard = self.offset.lock().unwrap();
        *offset_guard += 1;
        Ok(PollBatchResponse {
            record_count: 5,
            epoch: request.epoch,
            offset: format!("grpc-offset-{}", *offset_guard),
            watermark: request.epoch * 100,
        })
    }

    async fn discover_schema(
        &self,
        _request: DiscoverSchemaRequest,
    ) -> Result<DiscoverSchemaResponse, String> {
        let mut columns = BTreeMap::new();
        columns.insert(
            "event_count".to_string(),
            ColumnMetadata {
                law_id: 10, // PNCounter/v1
                crdt_type: "COUNTER".to_string(),
                write_classification: "blind_delta".to_string(),
            },
        );
        Ok(DiscoverSchemaResponse { columns })
    }

    async fn lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleResponse, String> {
        let mut state_guard = self.state.lock().unwrap();
        match request.action.as_str() {
            "get_state" => Ok(LifecycleResponse {
                success: true,
                state: format!("{:?}", *state_guard).to_lowercase(),
            }),
            "pause" => {
                if *state_guard == ConnectorLifecycleState::Running {
                    *state_guard = ConnectorLifecycleState::Paused;
                    Ok(LifecycleResponse {
                        success: true,
                        state: "paused".to_string(),
                    })
                } else {
                    Ok(LifecycleResponse {
                        success: false,
                        state: format!("{:?}", *state_guard).to_lowercase(),
                    })
                }
            }
            "resume" => {
                if *state_guard == ConnectorLifecycleState::Paused {
                    *state_guard = ConnectorLifecycleState::Running;
                    Ok(LifecycleResponse {
                        success: true,
                        state: "running".to_string(),
                    })
                } else {
                    Ok(LifecycleResponse {
                        success: false,
                        state: format!("{:?}", *state_guard).to_lowercase(),
                    })
                }
            }
            "delete" => {
                *state_guard = ConnectorLifecycleState::Deleted;
                Ok(LifecycleResponse {
                    success: true,
                    state: "deleted".to_string(),
                })
            }
            _ => Err("Invalid action".to_string()),
        }
    }
}
