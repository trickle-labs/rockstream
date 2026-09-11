//! Core component lifecycle architecture and NodeRuntime (ROADMAP §8.6, v0.62 Slices 5-7).

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::shutdown::ShutdownCoordinator;
use crate::CliError;
use rockstream_types::config::NodeConfig;
use rockstream_types::error_code::{RS_0002, RS_0003};
pub use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};

/// Bounded queue capacity for node lifecycle transition events.
pub const LIFECYCLE_EVENT_QUEUE_CAPACITY: usize = 1024;

/// Lifecycle transition event recorded by `NodeRuntime`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleEvent {
    pub component: &'static str,
    pub previous: LifecycleState,
    pub next: LifecycleState,
    pub timestamp_ms: u64,
}

/// Component lifecycle state (canonical 8 states from ROADMAP §8.6).
pub type ComponentState = LifecycleState;

/// Common interface for all runtime components.
#[async_trait::async_trait]
pub trait Component: Send + Sync {
    /// Return the static name of this component.
    fn name(&self) -> &'static str;

    /// Current lifecycle state of this component.
    fn state(&self) -> LifecycleState;

    /// Begin component startup and resource acquisition.
    async fn start(&mut self) -> Result<(), CliError>;

    /// Perform mandatory component recovery (WAL replay, lease acquisition, etc.).
    async fn recover(&mut self) -> Result<(), CliError>;

    /// Begin graceful draining: reject new requests and flush in-flight work.
    async fn drain(&mut self) -> Result<(), CliError>;

    /// Stop this component and release all acquired sockets and background tasks.
    async fn stop(&mut self) -> Result<(), CliError>;
}

/// Control-plane component managing topology, leases, and consensus.
pub struct ControlComponent {
    state: LifecycleState,
    fail_on_start: bool,
    fail_on_recover: bool,
}

impl ControlComponent {
    pub fn new() -> Self {
        Self {
            state: LifecycleState::Created,
            fail_on_start: false,
            fail_on_recover: false,
        }
    }

    pub fn with_start_failure(mut self, fail: bool) -> Self {
        self.fail_on_start = fail;
        self
    }

    pub fn with_recovery_failure(mut self, fail: bool) -> Self {
        self.fail_on_recover = fail;
        self
    }
}

impl Default for ControlComponent {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Component for ControlComponent {
    fn name(&self) -> &'static str {
        "control"
    }

    fn state(&self) -> LifecycleState {
        self.state
    }

    async fn start(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Starting;
        if self.fail_on_start {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "control component failed to start: bind failed",
                "Check control listening port availability.",
            ));
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Recovering;
        if self.fail_on_recover {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "control component mandatory recovery failed: consensus log replay error",
                "Check cluster persistent state integrity.",
            ));
        }
        self.state = LifecycleState::Ready;
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Draining;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Stopped;
        Ok(())
    }
}

/// Worker component managing execution shards, quantum scheduling, and caches.
pub struct WorkerComponent {
    state: LifecycleState,
    fail_on_start: bool,
    fail_on_recover: bool,
    storage_context: Arc<rockstream_storage::storage_context::WorkerStorageContext>,
}

impl WorkerComponent {
    pub fn new() -> Self {
        Self::with_cache_bytes(536_870_912)
    }

    pub fn with_cache_bytes(cache_bytes: usize) -> Self {
        Self {
            state: LifecycleState::Created,
            fail_on_start: false,
            fail_on_recover: false,
            storage_context: Arc::new(
                rockstream_storage::storage_context::WorkerStorageContext::new(cache_bytes),
            ),
        }
    }

    pub fn storage_context(
        &self,
    ) -> Arc<rockstream_storage::storage_context::WorkerStorageContext> {
        self.storage_context.clone()
    }

    pub fn with_start_failure(mut self, fail: bool) -> Self {
        self.fail_on_start = fail;
        self
    }

    pub fn with_recovery_failure(mut self, fail: bool) -> Self {
        self.fail_on_recover = fail;
        self
    }
}

impl Default for WorkerComponent {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Component for WorkerComponent {
    fn name(&self) -> &'static str {
        "worker"
    }

    fn state(&self) -> LifecycleState {
        self.state
    }

    async fn start(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Starting;
        if self.fail_on_start {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "worker component failed to start: failed to initialize storage shards",
                "Verify storage directory permissions and availability.",
            ));
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Recovering;
        if self.fail_on_recover {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "worker component mandatory recovery failed: shard state corruption",
                "Rebuild corrupt shard state or restore from clean backup.",
            ));
        }
        self.state = LifecycleState::Ready;
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Draining;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Stopped;
        Ok(())
    }
}

/// Gateway component serving PostgreSQL wire connections and queries.
pub struct GatewayComponent {
    state: LifecycleState,
    fail_on_start: bool,
    fail_on_recover: bool,
}

impl GatewayComponent {
    pub fn new() -> Self {
        Self {
            state: LifecycleState::Created,
            fail_on_start: false,
            fail_on_recover: false,
        }
    }

    pub fn with_start_failure(mut self, fail: bool) -> Self {
        self.fail_on_start = fail;
        self
    }

    pub fn with_recovery_failure(mut self, fail: bool) -> Self {
        self.fail_on_recover = fail;
        self
    }
}

impl Default for GatewayComponent {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Component for GatewayComponent {
    fn name(&self) -> &'static str {
        "gateway"
    }

    fn state(&self) -> LifecycleState {
        self.state
    }

    async fn start(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Starting;
        if self.fail_on_start {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "gateway component failed to start: port bind conflict",
                "Ensure listening port is not held by another process.",
            ));
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Recovering;
        if self.fail_on_recover {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "gateway component mandatory recovery failed",
                "Check gateway dependencies.",
            ));
        }
        self.state = LifecycleState::Ready;
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Draining;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Stopped;
        Ok(())
    }
}

/// Metrics and health monitoring HTTP component.
pub struct MetricsComponent {
    state: LifecycleState,
    fail_on_start: bool,
    fail_on_recover: bool,
}

impl MetricsComponent {
    pub fn new() -> Self {
        Self {
            state: LifecycleState::Created,
            fail_on_start: false,
            fail_on_recover: false,
        }
    }

    pub fn with_start_failure(mut self, fail: bool) -> Self {
        self.fail_on_start = fail;
        self
    }

    pub fn with_recovery_failure(mut self, fail: bool) -> Self {
        self.fail_on_recover = fail;
        self
    }
}

impl Default for MetricsComponent {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Component for MetricsComponent {
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn state(&self) -> LifecycleState {
        self.state
    }

    async fn start(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Starting;
        if self.fail_on_start {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "metrics component failed to start: address already in use",
                "Check metrics listen address.",
            ));
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Recovering;
        if self.fail_on_recover {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "metrics component mandatory recovery failed",
                "Verify metrics configuration.",
            ));
        }
        self.state = LifecycleState::Ready;
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Draining;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Stopped;
        Ok(())
    }
}

/// Connector supervisor component managing streaming data sources.
pub struct ConnectorSupervisor {
    state: LifecycleState,
    fail_on_start: bool,
    fail_on_recover: bool,
}

impl ConnectorSupervisor {
    pub fn new() -> Self {
        Self {
            state: LifecycleState::Created,
            fail_on_start: false,
            fail_on_recover: false,
        }
    }

    pub fn with_start_failure(mut self, fail: bool) -> Self {
        self.fail_on_start = fail;
        self
    }

    pub fn with_recovery_failure(mut self, fail: bool) -> Self {
        self.fail_on_recover = fail;
        self
    }
}

impl Default for ConnectorSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Component for ConnectorSupervisor {
    fn name(&self) -> &'static str {
        "connector_supervisor"
    }

    fn state(&self) -> LifecycleState {
        self.state
    }

    async fn start(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Starting;
        if self.fail_on_start {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "connector supervisor failed to start: source configuration invalid",
                "Check connector manifests.",
            ));
        }
        Ok(())
    }

    async fn recover(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Recovering;
        if self.fail_on_recover {
            self.state = LifecycleState::Fatal;
            return Err(CliError::new(
                RS_0003,
                "connector supervisor mandatory recovery failed: checkpoint offset mismatch",
                "Inspect source offsets or reset connector state.",
            ));
        }
        self.state = LifecycleState::Ready;
        Ok(())
    }

    async fn drain(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Draining;
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), CliError> {
        self.state = LifecycleState::Stopped;
        Ok(())
    }
}

/// Authoritative node runtime managing composed components and canonical lifecycle.
pub struct NodeRuntime {
    config: NodeConfig,
    tracker: Arc<LifecycleTracker>,
    coordinator: ShutdownCoordinator,
    components: Vec<Box<dyn Component>>,
    event_tx: mpsc::Sender<LifecycleEvent>,
    event_rx: Option<mpsc::Receiver<LifecycleEvent>>,
}

impl NodeRuntime {
    /// Instantiate a NodeRuntime from a validated `NodeConfig`.
    pub fn new(config: NodeConfig) -> Result<Self, CliError> {
        let role = config.node.role.to_lowercase();
        let tracker = Arc::new(LifecycleTracker::new(&role));
        let shutdown_timeout = Duration::from_secs(config.runtime.shutdown_timeout_secs);
        let coordinator = ShutdownCoordinator::new(tracker.clone(), shutdown_timeout);

        let (event_tx, event_rx) = mpsc::channel(LIFECYCLE_EVENT_QUEUE_CAPACITY);

        let mut runtime = Self {
            config,
            tracker,
            coordinator,
            components: Vec::new(),
            event_tx,
            event_rx: Some(event_rx),
        };

        runtime.compose_components_for_role(&role)?;
        Ok(runtime)
    }

    /// Compose identical component implementations according to the designated role.
    fn compose_components_for_role(&mut self, role: &str) -> Result<(), CliError> {
        match role {
            "all" => {
                self.components.push(Box::new(ControlComponent::new()));
                self.components.push(Box::new(WorkerComponent::new()));
                self.components.push(Box::new(GatewayComponent::new()));
                self.components.push(Box::new(MetricsComponent::new()));
                self.components.push(Box::new(ConnectorSupervisor::new()));
            }
            "control" => {
                self.components.push(Box::new(ControlComponent::new()));
                self.components.push(Box::new(MetricsComponent::new()));
            }
            "worker" => {
                self.components.push(Box::new(WorkerComponent::new()));
                self.components.push(Box::new(MetricsComponent::new()));
            }
            "gateway" => {
                self.components.push(Box::new(GatewayComponent::new()));
                self.components.push(Box::new(MetricsComponent::new()));
            }
            "metrics" => {
                self.components.push(Box::new(MetricsComponent::new()));
            }
            other => {
                return Err(CliError::new(
                    RS_0002,
                    format!("unsupported node role `{other}`"),
                    "Supported roles: 'all', 'control', 'worker', 'gateway', 'metrics'.",
                ));
            }
        }
        Ok(())
    }

    /// Custom runtime constructor with injected components for lifecycle and fault injection testing.
    pub fn with_components(config: NodeConfig, components: Vec<Box<dyn Component>>) -> Self {
        let role = config.node.role.to_lowercase();
        let tracker = Arc::new(LifecycleTracker::new(&role));
        let shutdown_timeout = Duration::from_secs(config.runtime.shutdown_timeout_secs);
        let coordinator = ShutdownCoordinator::new(tracker.clone(), shutdown_timeout);
        let (event_tx, event_rx) = mpsc::channel(LIFECYCLE_EVENT_QUEUE_CAPACITY);

        Self {
            config,
            tracker,
            coordinator,
            components,
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    pub fn tracker(&self) -> &Arc<LifecycleTracker> {
        &self.tracker
    }

    pub fn coordinator(&self) -> &ShutdownCoordinator {
        &self.coordinator
    }

    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn components(&self) -> &[Box<dyn Component>] {
        &self.components
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<LifecycleEvent>> {
        self.event_rx.take()
    }

    fn send_event(
        event_tx: &mpsc::Sender<LifecycleEvent>,
        component: &'static str,
        prev: LifecycleState,
        next: LifecycleState,
    ) {
        let event = LifecycleEvent {
            component,
            previous: prev,
            next,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        };
        let _ = event_tx.try_send(event);
    }

    /// Execute the strict lifecycle startup sequence:
    /// `Created -> Starting -> Recovering -> Ready`.
    /// On failure, teardown already-started components in reverse order and transition to `Fatal`.
    pub async fn start(&mut self) -> Result<(), CliError> {
        let event_tx = self.event_tx.clone();

        // 1. Enter Starting
        let prev = self.tracker.state();
        self.tracker
            .transition_to(LifecycleState::Starting)
            .map_err(|e| CliError::new(RS_0003, e, ""))?;
        Self::send_event(&event_tx, "node", prev, LifecycleState::Starting);

        let mut started_indices: Vec<usize> = Vec::new();

        // 2. Start components in forward order
        for (idx, comp) in self.components.iter_mut().enumerate() {
            let c_prev = comp.state();
            if let Err(e) = comp.start().await {
                error!(
                    component = comp.name(),
                    code = %RS_0003,
                    error = %e.message,
                    "[{}] Component startup failed — beginning reverse teardown",
                    RS_0003
                );
                Self::send_event(&event_tx, comp.name(), c_prev, LifecycleState::Fatal);
                self.tracker.set_state(LifecycleState::Fatal);
                for &s_idx in started_indices.iter().rev() {
                    let c: &mut Box<dyn Component> = &mut self.components[s_idx];
                    let c_prev = c.state();
                    let _ = c.stop().await;
                    Self::send_event(&event_tx, c.name(), c_prev, c.state());
                }
                return Err(e);
            }
            Self::send_event(&event_tx, comp.name(), c_prev, comp.state());
            started_indices.push(idx);
        }

        // 3. Enter Recovering
        let prev = self.tracker.state();
        self.tracker
            .transition_to(LifecycleState::Recovering)
            .map_err(|e| CliError::new(RS_0003, e, ""))?;
        Self::send_event(&event_tx, "node", prev, LifecycleState::Recovering);

        // 4. Perform mandatory recovery across components
        for &idx in &started_indices {
            let comp = &mut self.components[idx];
            let c_prev = comp.state();
            if let Err(e) = comp.recover().await {
                error!(
                    component = comp.name(),
                    code = %RS_0003,
                    error = %e.message,
                    "[{}] Component mandatory recovery failed — transitioning to Fatal and tearing down",
                    RS_0003
                );
                Self::send_event(&event_tx, comp.name(), c_prev, LifecycleState::Fatal);
                self.tracker.set_state(LifecycleState::Fatal);
                for &s_idx in started_indices.iter().rev() {
                    let c: &mut Box<dyn Component> = &mut self.components[s_idx];
                    let c_prev = c.state();
                    let _ = c.stop().await;
                    Self::send_event(&event_tx, c.name(), c_prev, c.state());
                }
                return Err(e);
            }
            Self::send_event(&event_tx, comp.name(), c_prev, comp.state());
        }

        // 5. Enter Ready
        let prev = self.tracker.state();
        self.tracker
            .transition_to(LifecycleState::Ready)
            .map_err(|e| CliError::new(RS_0003, e, ""))?;
        Self::send_event(&event_tx, "node", prev, LifecycleState::Ready);

        info!(
            role = %self.config.node.role,
            "NodeRuntime reached Ready state"
        );

        Ok(())
    }

    /// Graceful shutdown sequence:
    /// `Ready -> Draining -> Stopping -> Stopped`.
    pub async fn shutdown(&mut self) -> Result<(), CliError> {
        let event_tx = self.event_tx.clone();
        let prev = self.tracker.state();
        if prev != LifecycleState::Ready && prev != LifecycleState::Draining {
            warn!(current_state = ?prev, "Shutdown called when not Ready");
        }

        // 1. Enter Draining
        let _ = self.tracker.transition_to(LifecycleState::Draining);
        Self::send_event(&event_tx, "node", prev, LifecycleState::Draining);

        for comp in self.components.iter_mut() {
            let c_prev = comp.state();
            let _ = comp.drain().await;
            Self::send_event(&event_tx, comp.name(), c_prev, comp.state());
        }

        // 2. Enter Stopping
        let prev = self.tracker.state();
        let _ = self.tracker.transition_to(LifecycleState::Stopping);
        Self::send_event(&event_tx, "node", prev, LifecycleState::Stopping);

        // 3. Stop components in reverse dependency order
        for comp in self.components.iter_mut().rev() {
            let c_prev = comp.state();
            let _ = comp.stop().await;
            Self::send_event(&event_tx, comp.name(), c_prev, comp.state());
        }

        // 4. Enter Stopped
        let prev = self.tracker.state();
        let _ = self.tracker.transition_to(LifecycleState::Stopped);
        Self::send_event(&event_tx, "node", prev, LifecycleState::Stopped);
        self.coordinator.mark_completed();

        info!(
            role = %self.config.node.role,
            "NodeRuntime cleanly stopped"
        );
        Ok(())
    }
}
