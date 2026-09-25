//! Node lifecycle state machine and structured health definitions (v0.59.21).

use crate::candidate_identity::CandidateIdentity;
use crate::error_code::{ErrorCode, RS_0001};
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
                "[{}] Illegal lifecycle transition from {:?} to {:?}",
                RS_0001, self, next
            ))
        }
    }
}

/// Recovery phases during node startup/recovery before reaching `LifecycleState::Ready`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPhase {
    OpeningStorage,
    RecoveringCatalog,
    RecoveringEpoch,
    RecoveringOperators,
    ValidatingState,
    Ready,
}

impl RecoveryPhase {
    const CODE_OPENING_STORAGE: u8 = 1;
    const CODE_RECOVERING_CATALOG: u8 = 2;
    const CODE_RECOVERING_EPOCH: u8 = 3;
    const CODE_RECOVERING_OPERATORS: u8 = 4;
    const CODE_VALIDATING_STATE: u8 = 5;
    const CODE_READY: u8 = 6;

    pub fn to_u8(self) -> u8 {
        match self {
            Self::OpeningStorage => Self::CODE_OPENING_STORAGE,
            Self::RecoveringCatalog => Self::CODE_RECOVERING_CATALOG,
            Self::RecoveringEpoch => Self::CODE_RECOVERING_EPOCH,
            Self::RecoveringOperators => Self::CODE_RECOVERING_OPERATORS,
            Self::ValidatingState => Self::CODE_VALIDATING_STATE,
            Self::Ready => Self::CODE_READY,
        }
    }

    pub fn from_u8(code: u8) -> Option<Self> {
        match code {
            Self::CODE_OPENING_STORAGE => Some(Self::OpeningStorage),
            Self::CODE_RECOVERING_CATALOG => Some(Self::RecoveringCatalog),
            Self::CODE_RECOVERING_EPOCH => Some(Self::RecoveringEpoch),
            Self::CODE_RECOVERING_OPERATORS => Some(Self::RecoveringOperators),
            Self::CODE_VALIDATING_STATE => Some(Self::ValidatingState),
            Self::CODE_READY => Some(Self::Ready),
            _ => None,
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OpeningStorage => "opening_storage",
            Self::RecoveringCatalog => "recovering_catalog",
            Self::RecoveringEpoch => "recovering_epoch",
            Self::RecoveringOperators => "recovering_operators",
            Self::ValidatingState => "validating_state",
            Self::Ready => "ready",
        }
    }

    pub fn can_transition_to(&self, next: RecoveryPhase) -> bool {
        matches!(
            (*self, next),
            (Self::OpeningStorage, Self::RecoveringCatalog)
                | (Self::RecoveringCatalog, Self::RecoveringEpoch)
                | (Self::RecoveringEpoch, Self::RecoveringOperators)
                | (Self::RecoveringOperators, Self::ValidatingState)
                | (Self::ValidatingState, Self::Ready)
        )
    }

    pub fn transition_to(&self, next: RecoveryPhase) -> Result<(), String> {
        if self.can_transition_to(next) {
            Ok(())
        } else {
            Err(format!(
                "[{}] Illegal recovery phase transition from {:?} to {:?}",
                RS_0001, self, next
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

/// Individual dimension health evaluation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HealthDimensionStatus {
    Pass,
    Warn,
    Fail,
    Unknown,
}

impl HealthDimensionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Fail => "FAIL",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// An observation of a specific health dimension with provenance and timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthDimensionObservation {
    pub status: HealthDimensionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub sampled_at: u64,
    pub producer: String,
    pub is_stale: bool,
}

impl HealthDimensionObservation {
    pub fn new(
        status: HealthDimensionStatus,
        producer: impl Into<String>,
        sampled_at: u64,
    ) -> Self {
        Self {
            status,
            reason: None,
            sampled_at,
            producer: producer.into(),
            is_stale: false,
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Evaluates staleness against current timestamp (seconds).
    /// If sampled_at is older than threshold (15s), marks is_stale = true
    /// and downgrades status to UNKNOWN rather than reporting false healthy zeros.
    pub fn check_stale(&mut self, now_secs: u64, threshold_secs: u64) {
        if now_secs.saturating_sub(self.sampled_at) > threshold_secs {
            self.is_stale = true;
            self.status = HealthDimensionStatus::Unknown;
        }
    }
}

/// Seven independent health dimensions (Step 1, V071-01, UIE-M6-02).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthDimensions {
    pub liveness: HealthDimensionObservation,
    pub readiness: HealthDimensionObservation,
    pub availability: HealthDimensionObservation,
    pub freshness: HealthDimensionObservation,
    pub durability: HealthDimensionObservation,
    pub capacity: HealthDimensionObservation,
    pub degradation: HealthDimensionObservation,
}

impl HealthDimensions {
    pub const STALE_OBSERVATION_THRESHOLD_SECS: u64 = 15;

    pub fn new_initial(role: &str, now_secs: u64) -> Self {
        Self {
            liveness: HealthDimensionObservation::new(
                HealthDimensionStatus::Pass,
                format!("{role}:supervisor"),
                now_secs,
            ),
            readiness: HealthDimensionObservation::new(
                HealthDimensionStatus::Fail,
                format!("{role}:lifecycle"),
                now_secs,
            )
            .with_reason("Initializing"),
            availability: HealthDimensionObservation::new(
                HealthDimensionStatus::Pass,
                format!("{role}:gateway"),
                now_secs,
            ),
            freshness: HealthDimensionObservation::new(
                HealthDimensionStatus::Pass,
                format!("{role}:frontier"),
                now_secs,
            ),
            durability: HealthDimensionObservation::new(
                HealthDimensionStatus::Pass,
                format!("{role}:storage"),
                now_secs,
            ),
            capacity: HealthDimensionObservation::new(
                HealthDimensionStatus::Pass,
                format!("{role}:budget"),
                now_secs,
            ),
            degradation: HealthDimensionObservation::new(
                HealthDimensionStatus::Pass,
                format!("{role}:diagnostic"),
                now_secs,
            ),
        }
    }

    /// Check if overall workload health passes.
    /// Liveness alone NEVER produces workload health.
    pub fn is_overall_healthy(&self) -> bool {
        self.liveness.status == HealthDimensionStatus::Pass
            && self.readiness.status == HealthDimensionStatus::Pass
            && self.availability.status == HealthDimensionStatus::Pass
            && self.durability.status == HealthDimensionStatus::Pass
            && self.freshness.status != HealthDimensionStatus::Fail
            && self.capacity.status != HealthDimensionStatus::Fail
            && self.degradation.status != HealthDimensionStatus::Fail
    }

    /// Apply stale observation policy (15s threshold).
    pub fn apply_stale_policy(&mut self, now_secs: u64) {
        self.availability
            .check_stale(now_secs, Self::STALE_OBSERVATION_THRESHOLD_SECS);
        self.freshness
            .check_stale(now_secs, Self::STALE_OBSERVATION_THRESHOLD_SECS);
        self.durability
            .check_stale(now_secs, Self::STALE_OBSERVATION_THRESHOLD_SECS);
        self.capacity
            .check_stale(now_secs, Self::STALE_OBSERVATION_THRESHOLD_SECS);
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<HealthDimensions>,
}

/// Shared thread-safe tracker for node lifecycle, dependencies, and health state.
pub struct LifecycleTracker {
    role: String,
    state: AtomicU8,
    recovery_phase: AtomicU8,
    active_shards: AtomicUsize,
    start_time: Instant,
    identity: CandidateIdentity,
    dependencies: RwLock<BTreeMap<String, DependencyHealthReport>>,
    reasons: RwLock<Vec<HealthReason>>,
    dimensions: RwLock<HealthDimensions>,
}

impl LifecycleTracker {
    /// Create a new tracker starting in `LifecycleState::Created`.
    pub fn new(role: impl Into<String>) -> Self {
        Self::new_with_state(role, LifecycleState::Created)
    }

    /// Create a new tracker starting in an explicit state.
    pub fn new_with_state(role: impl Into<String>, state: LifecycleState) -> Self {
        let role_str = role.into();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            role: role_str.clone(),
            state: AtomicU8::new(state.to_u8()),
            recovery_phase: AtomicU8::new(0),
            active_shards: AtomicUsize::new(0),
            start_time: Instant::now(),
            identity: CandidateIdentity::current(),
            dependencies: RwLock::new(BTreeMap::new()),
            reasons: RwLock::new(Vec::new()),
            dimensions: RwLock::new(HealthDimensions::new_initial(&role_str, now_secs)),
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

    pub fn recovery_phase(&self) -> Option<RecoveryPhase> {
        RecoveryPhase::from_u8(self.recovery_phase.load(Ordering::SeqCst))
    }

    pub fn set_recovery_phase(&self, phase: RecoveryPhase) {
        self.recovery_phase.store(phase.to_u8(), Ordering::SeqCst);
        if phase.is_ready() {
            self.set_state(LifecycleState::Ready);
        } else {
            self.set_state(LifecycleState::Recovering);
        }
    }

    pub fn transition_recovery_phase(&self, next: RecoveryPhase) -> Result<(), String> {
        if let Some(current) = self.recovery_phase() {
            current.transition_to(next)?;
        }
        self.set_recovery_phase(next);
        Ok(())
    }

    pub fn fail_recovery(&self, reason: HealthReason) {
        self.add_reason(reason);
        self.set_state(LifecycleState::Fatal);
    }

    pub fn transition_to(&self, next: LifecycleState) -> Result<(), String> {
        let current = self.state();
        if current.can_transition_to(next) {
            self.set_state(next);
            Ok(())
        } else {
            Err(format!(
                "[{}] Illegal lifecycle transition from {:?} to {:?}",
                RS_0001, current, next
            ))
        }
    }

    pub fn is_alive(&self) -> bool {
        self.state().is_alive()
    }

    pub fn is_ready(&self) -> bool {
        if let Some(phase) = self.recovery_phase() {
            if !phase.is_ready() {
                return false;
            }
        }
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

    pub fn dimensions(&self) -> HealthDimensions {
        let mut dims = self.dimensions.read().clone();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        dims.liveness.status = if self.is_alive() {
            HealthDimensionStatus::Pass
        } else {
            HealthDimensionStatus::Fail
        };
        dims.liveness.sampled_at = now_secs;
        dims.liveness.is_stale = false;

        dims.readiness.status = if self.is_ready() {
            HealthDimensionStatus::Pass
        } else {
            HealthDimensionStatus::Fail
        };
        dims.readiness.sampled_at = now_secs;
        dims.readiness.is_stale = false;
        if !self.is_ready() {
            dims.readiness.reason = Some(self.state().as_str().to_string());
        } else {
            dims.readiness.reason = None;
        }
        dims
    }

    pub fn set_durability_status(
        &self,
        status: HealthDimensionStatus,
        reason: Option<String>,
        sampled_at: u64,
    ) {
        let mut dims = self.dimensions.write();
        dims.durability.status = status;
        dims.durability.reason = reason;
        dims.durability.sampled_at = sampled_at;
        dims.durability.is_stale = false;
    }

    pub fn set_availability_status(
        &self,
        status: HealthDimensionStatus,
        reason: Option<String>,
        sampled_at: u64,
    ) {
        let mut dims = self.dimensions.write();
        dims.availability.status = status;
        dims.availability.reason = reason;
        dims.availability.sampled_at = sampled_at;
        dims.availability.is_stale = false;
    }

    pub fn set_freshness_status(
        &self,
        status: HealthDimensionStatus,
        reason: Option<String>,
        sampled_at: u64,
    ) {
        let mut dims = self.dimensions.write();
        dims.freshness.status = status;
        dims.freshness.reason = reason;
        dims.freshness.sampled_at = sampled_at;
        dims.freshness.is_stale = false;
    }

    pub fn set_capacity_status(
        &self,
        status: HealthDimensionStatus,
        reason: Option<String>,
        sampled_at: u64,
    ) {
        let mut dims = self.dimensions.write();
        dims.capacity.status = status;
        dims.capacity.reason = reason;
        dims.capacity.sampled_at = sampled_at;
        dims.capacity.is_stale = false;
    }

    pub fn set_degradation_status(
        &self,
        status: HealthDimensionStatus,
        reason: Option<String>,
        sampled_at: u64,
    ) {
        let mut dims = self.dimensions.write();
        dims.degradation.status = status;
        dims.degradation.reason = reason;
        dims.degradation.sampled_at = sampled_at;
        dims.degradation.is_stale = false;
    }

    pub fn update_dimensions<F: FnOnce(&mut HealthDimensions)>(&self, f: F) {
        let mut dims = self.dimensions.write();
        f(&mut dims);
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
        let mut code = state.health_http_code();
        let dependencies = self.dependencies.read().clone();
        let reasons = self.reasons.read().clone();

        let mut dimensions = self.dimensions.read().clone();
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update instantaneous liveness and readiness
        dimensions.liveness.status = if self.is_alive() {
            HealthDimensionStatus::Pass
        } else {
            HealthDimensionStatus::Fail
        };
        dimensions.liveness.sampled_at = now_secs;
        dimensions.liveness.is_stale = false;

        dimensions.readiness.status = if self.is_ready() {
            HealthDimensionStatus::Pass
        } else {
            HealthDimensionStatus::Fail
        };
        dimensions.readiness.sampled_at = now_secs;
        dimensions.readiness.is_stale = false;
        if !self.is_ready() {
            dimensions.readiness.reason = Some(state.as_str().to_string());
        } else {
            dimensions.readiness.reason = None;
        }

        // Apply stale observation policy (15s threshold)
        dimensions.apply_stale_policy(now_secs);

        // Grounding Rule: A live process with unavailable storage MUST remain live while reporting durability: FAIL and availability: FAIL.
        // Liveness alone NEVER produces workload health.
        if (dimensions.durability.status == HealthDimensionStatus::Fail
            || dimensions.availability.status == HealthDimensionStatus::Fail)
            && code == 200
        {
            code = 503;
        }

        let report_status = if dimensions.durability.status == HealthDimensionStatus::Fail
            || dimensions.availability.status == HealthDimensionStatus::Fail
        {
            "unhealthy".to_string()
        } else {
            state.health_status_str().to_string()
        };

        let report = HealthReport {
            status: report_status,
            role: self.role.clone(),
            version: self.identity.semantic_version.clone(),
            commit_sha: self.identity.commit_sha.clone(),
            uptime_secs: self.uptime_secs(),
            dependencies,
            active_shards: self.active_shards(),
            reasons,
            dimensions: Some(dimensions),
        };

        (code, report)
    }
}
