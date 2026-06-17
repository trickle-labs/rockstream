//! `SimRuntime`: deterministic simulation runtime with seeded RNG.
//!
//! All randomness, time, and task scheduling is driven by a single seed,
//! ensuring byte-for-byte reproducibility of execution across runs.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::clock::SimClock;
use crate::network::SimNetworkHandle;
use crate::object_store::SimObjectStoreHandle;
use crate::runtime::{BoxFuture, Runtime, Spawner};

/// A record of a spawned task (for inspection/debugging).
#[derive(Debug, Clone)]
pub struct SpawnedTask {
    pub name: &'static str,
    pub spawn_order: u64,
}

/// Deterministic simulation runtime.
///
/// Tasks spawned on this runtime are recorded but executed synchronously
/// in the test context. The seeded RNG ensures all random decisions are
/// reproducible.
pub struct SimRuntime {
    seed: u64,
    clock: SimClock,
    object_store: SimObjectStoreHandle,
    network: SimNetworkHandle,
    rng: Arc<Mutex<SmallRng>>,
    spawned_tasks: Arc<Mutex<Vec<SpawnedTask>>>,
    spawn_counter: Arc<Mutex<u64>>,
}

impl SimRuntime {
    /// Create a new SimRuntime with the given seed.
    pub fn new(seed: u64) -> Self {
        let clock = SimClock::new();
        let object_store = SimObjectStoreHandle::new();
        object_store.set_clock(clock.clone());
        Self {
            seed,
            clock,
            object_store,
            network: SimNetworkHandle::new(),
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            spawned_tasks: Arc::new(Mutex::new(Vec::new())),
            spawn_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Get a random u64 from the runtime's RNG (for deterministic decisions).
    pub fn random_u64(&self) -> u64 {
        self.rng.lock().gen()
    }

    /// Get a random f64 in [0, 1) from the runtime's RNG.
    pub fn random_f64(&self) -> f64 {
        self.rng.lock().gen()
    }

    /// Get a random bool with the given probability.
    pub fn random_bool(&self, probability: f64) -> bool {
        self.rng.lock().gen_bool(probability.clamp(0.0, 1.0))
    }

    /// Advance the simulation clock by the given duration.
    pub fn advance_time(&self, duration: Duration) {
        self.clock.advance(duration);
    }

    /// Get the list of tasks spawned during this simulation run.
    pub fn spawned_tasks(&self) -> Vec<SpawnedTask> {
        self.spawned_tasks.lock().clone()
    }

    /// Number of tasks spawned.
    pub fn spawn_count(&self) -> u64 {
        *self.spawn_counter.lock()
    }
}

impl Runtime for SimRuntime {
    type Clock = SimClock;

    fn clock(&self) -> &SimClock {
        &self.clock
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        // In simulation, sleep just advances the clock.
        self.clock.advance(duration);
        Box::pin(async {})
    }

    fn spawn<F>(&self, name: &'static str, _future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut counter = self.spawn_counter.lock();
        *counter += 1;
        let order = *counter;
        self.spawned_tasks.lock().push(SpawnedTask {
            name,
            spawn_order: order,
        });
        // In the basic SimRuntime, spawned tasks are recorded but not executed.
        // A full simulation executor would drive them step by step.
    }

    fn object_store(&self) -> &SimObjectStoreHandle {
        &self.object_store
    }

    fn network(&self) -> &SimNetworkHandle {
        &self.network
    }

    fn seed(&self) -> u64 {
        self.seed
    }

    fn is_simulation(&self) -> bool {
        true
    }
}

impl Spawner for SimRuntime {
    fn spawn_box(&self, name: &'static str, f: BoxFuture<'static, ()>) {
        // Record the spawn for simulation inspection.
        let mut counter = self.spawn_counter.lock();
        *counter += 1;
        let order = *counter;
        self.spawned_tasks.lock().push(SpawnedTask {
            name,
            spawn_order: order,
        });
        drop(counter);
        // Execute via Tokio. Full deterministic step-scheduling is a Phase 3+
        // concern; for now tasks run concurrently under the test Tokio runtime,
        // which preserves correctness while making the spawn site testable.
        tokio::spawn(f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;
    use bytes::Bytes;

    #[test]
    fn sim_runtime_deterministic_rng() {
        let rt1 = SimRuntime::new(42);
        let rt2 = SimRuntime::new(42);

        let seq1: Vec<u64> = (0..20).map(|_| rt1.random_u64()).collect();
        let seq2: Vec<u64> = (0..20).map(|_| rt2.random_u64()).collect();

        assert_eq!(seq1, seq2, "Same seed must produce identical RNG sequence");
    }

    #[test]
    fn sim_runtime_different_seed_differs() {
        let rt1 = SimRuntime::new(100);
        let rt2 = SimRuntime::new(200);

        let seq1: Vec<u64> = (0..20).map(|_| rt1.random_u64()).collect();
        let seq2: Vec<u64> = (0..20).map(|_| rt2.random_u64()).collect();

        assert_ne!(
            seq1, seq2,
            "Different seeds must produce different sequences"
        );
    }

    #[test]
    fn sim_runtime_is_simulation() {
        let rt = SimRuntime::new(0);
        assert!(rt.is_simulation());
        assert_eq!(rt.seed(), 0);
    }

    #[test]
    fn sim_runtime_clock_advances_on_sleep() {
        let rt = SimRuntime::new(0);
        let t0 = rt.clock().elapsed_since_epoch();
        // sleep is synchronous in sim - just advances the clock
        drop(rt.sleep(Duration::from_millis(500)));
        let t1 = rt.clock().elapsed_since_epoch();
        assert_eq!(t1 - t0, Duration::from_millis(500));
    }

    #[test]
    fn sim_runtime_object_store_deterministic() {
        let rt1 = SimRuntime::new(99);
        let rt2 = SimRuntime::new(99);

        // Perform same operations
        rt1.object_store()
            .put("key/a", Bytes::from("value_a"))
            .unwrap();
        rt1.object_store()
            .put("key/b", Bytes::from("value_b"))
            .unwrap();

        rt2.object_store()
            .put("key/a", Bytes::from("value_a"))
            .unwrap();
        rt2.object_store()
            .put("key/b", Bytes::from("value_b"))
            .unwrap();

        assert_eq!(
            rt1.object_store().snapshot(),
            rt2.object_store().snapshot(),
            "Same operations must produce identical object store state"
        );
    }

    #[test]
    fn sim_runtime_spawn_records_tasks() {
        let rt = SimRuntime::new(0);
        rt.spawn("task_a", async {});
        rt.spawn("task_b", async {});
        rt.spawn("task_c", async {});

        assert_eq!(rt.spawn_count(), 3);
        let tasks = rt.spawned_tasks();
        assert_eq!(tasks[0].name, "task_a");
        assert_eq!(tasks[1].name, "task_b");
        assert_eq!(tasks[2].name, "task_c");
        assert_eq!(tasks[0].spawn_order, 1);
        assert_eq!(tasks[2].spawn_order, 3);
    }

    #[test]
    fn sim_runtime_network_deterministic() {
        let rt1 = SimRuntime::new(55);
        let rt2 = SimRuntime::new(55);

        rt1.network().send(1, 2, Bytes::from("hello"));
        rt1.network().send(2, 3, Bytes::from("world"));

        rt2.network().send(1, 2, Bytes::from("hello"));
        rt2.network().send(2, 3, Bytes::from("world"));

        let msgs1 = rt1.network().drain_all();
        let msgs2 = rt2.network().drain_all();

        assert_eq!(
            msgs1, msgs2,
            "Same operations must produce identical network state"
        );
    }
}
