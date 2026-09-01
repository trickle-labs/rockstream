//! Unified Graceful Shutdown Coordinator and Watchdog (v0.59.21).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use rockstream_types::error_code::RS_3023;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};

pub static SUPPRESS_PROCESS_EXIT: AtomicBool = AtomicBool::new(false);

/// Coordinates graceful shutdown ordering and deadline watchdog enforcement across all roles.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    tracker: Arc<LifecycleTracker>,
    shutdown_timeout: Duration,
    shutdown_tx: broadcast::Sender<()>,
    is_shutting_down: Arc<AtomicBool>,
    is_completed: Arc<AtomicBool>,
}

impl ShutdownCoordinator {
    /// Create a new coordinator with an explicit tracker and timeout.
    pub fn new(tracker: Arc<LifecycleTracker>, shutdown_timeout: Duration) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        Self {
            tracker,
            shutdown_timeout,
            shutdown_tx,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            is_completed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn tracker(&self) -> &Arc<LifecycleTracker> {
        &self.tracker
    }

    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    /// Trigger graceful shutdown manually (e.g. from an API request or test).
    pub fn trigger_shutdown(&self) {
        if !self.is_shutting_down.swap(true, Ordering::SeqCst) {
            info!(
                role = %self.tracker.role(),
                "Graceful shutdown triggered — entering Draining state"
            );
            self.tracker.set_state(LifecycleState::Draining);
            let _ = self.shutdown_tx.send(());
        }
    }

    /// Subscribe to the shutdown notification channel.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Mark shutdown as successfully completed (cancels watchdog panic/exit).
    pub fn mark_completed(&self) {
        self.is_completed.store(true, Ordering::SeqCst);
        self.tracker.set_state(LifecycleState::Terminated);
    }

    /// Spawns an asynchronous watchdog timer. If shutdown takes longer than
    /// `shutdown_timeout`, the node logs fatal `RS-3023` and terminates.
    pub fn spawn_watchdog(&self) -> tokio::task::JoinHandle<()> {
        let timeout = self.shutdown_timeout;
        let is_completed = self.is_completed.clone();
        let tracker = self.tracker.clone();

        tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            if !is_completed.load(Ordering::SeqCst) {
                error!(
                    role = %tracker.role(),
                    code = %RS_3023,
                    timeout_secs = timeout.as_secs(),
                    "FATAL: Shutdown deadline exceeded without clean completion (RS-3023). Self-fencing and forcing process exit."
                );
                tracker.set_state(LifecycleState::Fatal);

                if !SUPPRESS_PROCESS_EXIT.load(Ordering::SeqCst)
                    && std::env::var("ROCKSTREAM_TEST_NO_EXIT").is_err()
                    && !cfg!(test)
                {
                    std::process::exit(1);
                }
            }
        })
    }

    /// Block until SIGINT (Ctrl-C), SIGTERM, or a manual `trigger_shutdown()`.
    pub async fn wait_for_signal_or_trigger(&self) {
        let mut rx = self.subscribe();

        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("Failed to register SIGTERM handler: {e}");
                    None
                }
            };

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("SIGINT (Ctrl-C) caught — initiating graceful shutdown");
                }
                _ = async {
                    if let Some(st) = sigterm.as_mut() {
                        st.recv().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    info!("SIGTERM caught — initiating graceful shutdown");
                }
                _ = rx.recv() => {
                    info!("Shutdown trigger received on coordinator");
                }
            }
        }

        #[cfg(not(unix))]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("SIGINT (Ctrl-C) caught — initiating graceful shutdown");
                }
                _ = rx.recv() => {
                    info!("Shutdown trigger received on coordinator");
                }
            }
        }

        self.trigger_shutdown();
    }
}
