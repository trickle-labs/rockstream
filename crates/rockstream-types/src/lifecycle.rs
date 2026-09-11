//! Node lifecycle state machine and structured health definitions (v0.59.21).

use crate::candidate_identity::CandidateIdentity;
use crate::error_code::ErrorCode;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;

/// Explicit role lifecycle states across all RockStream roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Created,
    Starting,
    Recovering,
    Ready,
    Draining,
    Stopping,
    Stopped,
    Fatal,

    #[deprecated(note = "Legacy state; use Ready with HealthReason")]
    Degraded,
    #[deprecated(note = "Legacy state; use Fatal")]
    DependencyLoss,
    #[deprecated(note = "Legacy state; use Stopping")]
    ShuttingDown,
    #[deprecated(note = "Legacy state; use Stopped")]
    Terminated,
}

impl LifecycleState {
    const CODE_CREATED: u8 = 0;
    const CODE_STARTING: u8 = 1;
    const CODE_RECOVERING: u8 = 2;
    const CODE_READY: u8 = 3;
    const CODE_DRAINING: u8 = 4;
    const CODE_STOPPING: u8 = 5;
    const CODE_STOPPED: u8 = 6;
    const CODE_FATAL: u8 = 7;
    const CODE_DEGRADED: u8 = 8;
    const CODE_DEPENDENCY_LOSS: u8 = 9;
    const CODE_SHUTTING_DOWN: u8 = 10;
    const CODE_TERMINATED: u8 = 11;

    #[allow(deprecated)]
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Created => Self::CODE_CREATED,
            Self::Starting => Self::CODE_STARTING,
            Self::Recovering => Self::CODE_RECOVERING,
            Self::Ready => Self::CODE_READY,
            Self::Draining => Self::CODE_DRAINING,
            Self::Stopping => Self::CODE_STOPPING,
            Self::Stopped => Self::CODE_STOPPED,
            Self::Fatal => Self::CODE_FATAL,
            Self::Degraded => Self::CODE_DEGRADED,
            Self::DependencyLoss => Self::CODE_DEPENDENCY_LOSS,
            Self::ShuttingDown => Self::CODE_SHUTTING_DOWN,
            Self::Terminated => Self::CODE_TERMINATED,
        }
    }

    #[allow(deprecated)]
    pub fn from_u8(code: u8) -> Self {
        match code {
            Self::CODE_CREATED => Self::Created,
            Self::CODE_STARTING => Self::Starting,
            Self::CODE_RECOVERING => Self::Recovering,
            Self::CODE_READY => Self::Ready,
            Self::CODE_DRAINING => Self::Draining,
            Self::CODE_STOPPING => Self::Stopping,
            Self::CODE_STOPPED => Self::Stopped,
            Self::CODE_FATAL => Self::Fatal,
            Self::CODE_DEGRADED => Self::Degraded,
            Self::CODE_DEPENDENCY_LOSS => Self::DependencyLoss,
            Self::CODE_SHUTTING_DOWN => Self::ShuttingDown,
            Self::CODE_TERMINATED => Self::Terminated,
            _ => Self::Fatal,
        }
    }

    #[allow(deprecated)]
    pub fn is_alive(&self) -> bool {
        !matches!(
            self,
            LifecycleState::Stopped | LifecycleState::Fatal | LifecycleState::Terminated
        )
    }

    #[allow(deprecated)]
    pub fn is_ready(&self) -> bool {
        matches!(self, LifecycleState::Ready | LifecycleState::Degraded)
    }

    #[allow(deprecated)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Recovering => "recovering",
            Self::Ready => "ready",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Fatal => "fatal",
            Self::Degraded => "degraded",
            Self::DependencyLoss => "dependency_loss",
            Self::ShuttingDown => "shutting_down",
            Self::Terminated => "terminated",
        }
    }

    #[allow(deprecated)]
    pub fn health_status_str(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Recovering => "recovering",
            Self::Ready => "healthy",
            Self::Draining => "draining",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Fatal => "fatal",
            Self::Degraded => "degraded",
            Self::DependencyLoss => "unhealthy",
            Self::ShuttingDown => "shutting_down",
            Self::Terminated => "terminated",
        }
    }

    pub fn ready_http_code(&self) -> u16 {
        if self.is_ready() {
            200
        } else {
            503
        }
    }

    #[allow(deprecated)]
    pub fn live_http_code(&self) -> u16 {
        match self {
            Self::Fatal => 500,
            Self::Stopped | Self::Terminated => 503,
            _ => 200,
        }
    }

    #[allow(deprecated)]
    pub fn health_http_code(&self) -> u16 {
        match self {
            Self::Ready | Self::Degraded => 200,
            Self::Fatal => 500,
            _ => 503,
        }
    }

    #[allow(deprecated)]
    pub fn can_transition_to(&self, next: LifecycleState) -> bool {
        match (*self, next) {
            (s, LifecycleState::Fatal)
                if s != LifecycleState::Fatal
                    && s != LifecycleState::Stopped
                    && s != LifecycleState::Terminated =>
            {
                true
            }
            (LifecycleState::Created, LifecycleState::Starting) => true,
            (LifecycleState::Starting, LifecycleState::Recovering) => true,
            (LifecycleState::Recovering, LifecycleState::Ready) => true,
            (LifecycleState::Ready, LifecycleState::Draining) => true,
            (LifecycleState::Draining, LifecycleState::Stopping) => true,
            (LifecycleState::Stopping, LifecycleState::Stopped) => true,
            // Legacy transitions
            (LifecycleState::Ready, LifecycleState::Degraded) => true,
            (LifecycleState::Degraded, LifecycleState::Ready) => true,
            (LifecycleState::Ready, LifecycleState::DependencyLoss) => true,
            (LifecycleState::Draining, LifecycleState::ShuttingDown) => true,
            (LifecycleState::ShuttingDown, LifecycleState::Terminated) => true,
            _ => false,
        }
    }

    pub fn transition_to(&self, next: LifecycleState) -> Result<(), String> {
        if self.can_transition_to(next) {
            Ok(())
        } else {
            Err(format!(
                "Illegal lifecycle transition from {:?} to {:?}",
                self, next
            ))
        }
    }
}

/// Dependency health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyStatus {
    Ok,
    Degraded,
    Unhealthy,
}

impl DependencyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }
}

/// Structured health status for an external or internal dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyHealthReport {
    pub name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

/// Actionable degradation reason or health diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReason {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
}

impl HealthReason {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            component: None,
        }
    }

    pub fn with_component(mut self, component: impl Into<String>) -> Self {
        self.component = Some(component.into());
        self
    }
}

/// JSON body returned by `/live` endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveResponse {
    pub status: String,
}

/// JSON body returned by `/ready` endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Comprehensive JSON body returned by `/health` endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReport {
    pub status: String,
    pub role: String,
    pub version: String,
    pub commit_sha: String,
    pub uptime_secs: u64,
    pub dependencies: BTreeMap<String, DependencyHealthReport>,
    pub active_shards: usize,
    pub reasons: Vec<HealthReason>,
}

/// Shared thread-safe tracker for node lifecycle, dependencies, and health state.
pub struct LifecycleTracker {
    role: String,
    state: AtomicU8,
    active_shards: AtomicUsize,
    start_time: Instant,
    identity: CandidateIdentity,
    dependencies: RwLock<BTreeMap<String, DependencyHealthReport>>,
    reasons: RwLock<Vec<HealthReason>>,
}

impl LifecycleTracker {
    /// Create a new tracker starting in `LifecycleState::Created`.
    pub fn new(role: impl Into<String>) -> Self {
        Self::new_with_state(role, LifecycleState::Created)
    }

    /// Create a new tracker starting in an explicit state.
    pub fn new_with_state(role: impl Into<String>, state: LifecycleState) -> Self {
        Self {
            role: role.into(),
            state: AtomicU8::new(state.to_u8()),
            active_shards: AtomicUsize::new(0),
            start_time: Instant::now(),
            identity: CandidateIdentity::current(),
            dependencies: RwLock::new(BTreeMap::new()),
            reasons: RwLock::new(Vec::new()),
        }
    }

    pub fn role(&self) -> &str {
        &self.role
    }

    pub fn state(&self) -> LifecycleState {
        LifecycleState::from_u8(self.state.load(Ordering::SeqCst))
    }

    pub fn set_state(&self, state: LifecycleState) {
        self.state.store(state.to_u8(), Ordering::SeqCst);
    }

    pub fn transition_to(&self, next: LifecycleState) -> Result<(), String> {
        let current = self.state();
        if current.can_transition_to(next) {
            self.set_state(next);
            Ok(())
        } else {
            Err(format!(
                "Illegal lifecycle transition from {:?} to {:?}",
                current, next
            ))
        }
    }

    pub fn is_alive(&self) -> bool {
        self.state().is_alive()
    }

    pub fn is_ready(&self) -> bool {
        self.state().is_ready()
    }

    pub fn set_active_shards(&self, count: usize) {
        self.active_shards.store(count, Ordering::SeqCst);
    }

    pub fn active_shards(&self) -> usize {
        self.active_shards.load(Ordering::SeqCst)
    }

    pub fn set_dependency(
        &self,
        name: impl Into<String>,
        status: DependencyStatus,
        message: Option<String>,
        latency_ms: Option<u64>,
    ) {
        let name_str = name.into();
        let report = DependencyHealthReport {
            name: name_str.clone(),
            status: status.as_str().to_string(),
            message,
            latency_ms,
        };
        self.dependencies.write().insert(name_str, report);
    }

    pub fn remove_dependency(&self, name: &str) {
        self.dependencies.write().remove(name);
    }

    pub fn add_reason(&self, reason: HealthReason) {
        self.reasons.write().push(reason);
    }

    pub fn clear_reasons(&self) {
        self.reasons.write().clear();
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn generate_live_response(&self) -> (u16, LiveResponse) {
        let state = self.state();
        let code = state.live_http_code();
        let status = if code == 200 {
            "alive".to_string()
        } else {
            state.as_str().to_string()
        };
        (code, LiveResponse { status })
    }

    pub fn generate_ready_response(&self) -> (u16, ReadyResponse) {
        let state = self.state();
        if state.is_ready() {
            (
                200,
                ReadyResponse {
                    status: "ready".to_string(),
                    reason: None,
                },
            )
        } else {
            (
                503,
                ReadyResponse {
                    status: "not_ready".to_string(),
                    reason: Some(state.as_str().to_string()),
                },
            )
        }
    }

    pub fn generate_health_report(&self) -> (u16, HealthReport) {
        let state = self.state();
        let code = state.health_http_code();
        let dependencies = self.dependencies.read().clone();
        let reasons = self.reasons.read().clone();

        let report = HealthReport {
            status: state.health_status_str().to_string(),
            role: self.role.clone(),
            version: self.identity.semantic_version.clone(),
            commit_sha: self.identity.commit_sha.clone(),
            uptime_secs: self.uptime_secs(),
            dependencies,
            active_shards: self.active_shards(),
            reasons,
        };

        (code, report)
    }
}
