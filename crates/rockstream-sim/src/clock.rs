//! Clock abstraction for deterministic time control.

use std::time::{Duration, Instant};

/// A clock that can be controlled in simulation or backed by real time.
pub trait Clock: Send + Sync + 'static {
    /// Returns the current instant.
    fn now(&self) -> Instant;

    /// Returns the elapsed time since an arbitrary epoch (monotonic).
    fn elapsed_since_epoch(&self) -> Duration;
}

/// Real clock backed by `std::time::Instant`.
#[derive(Debug, Clone)]
pub struct TokioClock {
    epoch: Instant,
}

impl TokioClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn elapsed_since_epoch(&self) -> Duration {
        self.epoch.elapsed()
    }
}

use std::sync::Arc;

/// Simulation clock with manually advanceable time.
#[derive(Debug, Clone)]
pub struct SimClock {
    epoch: Instant,
    offset: Arc<parking_lot::Mutex<Duration>>,
}

impl SimClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
            offset: Arc::new(parking_lot::Mutex::new(Duration::ZERO)),
        }
    }

    /// Advance the simulated clock by the given duration.
    pub fn advance(&self, duration: Duration) {
        let mut offset = self.offset.lock();
        *offset += duration;
    }

    /// Get the current simulated offset from the epoch.
    pub fn current_offset(&self) -> Duration {
        *self.offset.lock()
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        self.epoch + *self.offset.lock()
    }

    fn elapsed_since_epoch(&self) -> Duration {
        *self.offset.lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sim_clock_advance() {
        let clock = SimClock::new();
        let t0 = clock.elapsed_since_epoch();
        clock.advance(Duration::from_millis(100));
        let t1 = clock.elapsed_since_epoch();
        assert_eq!(t1 - t0, Duration::from_millis(100));
    }

    #[test]
    fn sim_clock_starts_at_zero() {
        let clock = SimClock::new();
        assert_eq!(clock.elapsed_since_epoch(), Duration::ZERO);
    }

    #[test]
    fn tokio_clock_monotonic() {
        let clock = TokioClock::new();
        let t0 = clock.elapsed_since_epoch();
        let t1 = clock.elapsed_since_epoch();
        assert!(t1 >= t0);
    }
}
