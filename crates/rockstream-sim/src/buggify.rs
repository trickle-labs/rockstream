//! The `buggify!()` macro for fault injection.
//!
//! When the `simulation` feature is enabled, `buggify!()` uses a thread-local
//! RNG to probabilistically inject faults. In production builds (no `simulation`
//! feature), it compiles to a no-op that always returns `false`.
//!
//! Every `buggify!()` call site must name a fault-model entry.

use std::cell::RefCell;

use rand::rngs::SmallRng;
#[cfg(feature = "simulation")]
use rand::Rng;
use rand::SeedableRng;

thread_local! {
    static BUGGIFY_RNG: RefCell<Option<SmallRng>> = const { RefCell::new(None) };
    static BUGGIFY_ACTIVE: RefCell<bool> = const { RefCell::new(false) };
    static BUGGIFY_FOCUS: RefCell<Option<&'static str>> = const { RefCell::new(None) };
}

/// Initialize buggify for the current thread with the given seed.
/// This enables fault injection on this thread.
pub fn buggify_init(seed: u64) {
    BUGGIFY_RNG.with(|rng| {
        *rng.borrow_mut() = Some(SmallRng::seed_from_u64(seed));
    });
    BUGGIFY_ACTIVE.with(|active| {
        *active.borrow_mut() = true;
    });
}

/// Disable buggify on the current thread.
pub fn buggify_disable() {
    BUGGIFY_ACTIVE.with(|active| {
        *active.borrow_mut() = false;
    });
    BUGGIFY_FOCUS.with(|focus| {
        *focus.borrow_mut() = None;
    });
}

/// Check if buggify is currently enabled on this thread.
pub fn buggify_enabled() -> bool {
    BUGGIFY_ACTIVE.with(|active| *active.borrow())
}

/// Restrict fault injection to a single fault ID on the current thread.
pub fn buggify_focus(fault_id: &'static str) {
    BUGGIFY_FOCUS.with(|focus| {
        *focus.borrow_mut() = Some(fault_id);
    });
}

/// Core buggify check: returns true with the given probability if simulation
/// is enabled. Always returns false in production builds.
#[cfg(feature = "simulation")]
pub fn buggify_check(probability: f64, _fault_id: &'static str) -> bool {
    if !buggify_enabled() {
        return false;
    }
    let focused = BUGGIFY_FOCUS.with(|focus| *focus.borrow());
    if focused.is_some_and(|fault_id| fault_id != _fault_id) {
        return false;
    }
    BUGGIFY_RNG.with(|rng| {
        let mut rng = rng.borrow_mut();
        match rng.as_mut() {
            Some(r) => r.gen_bool(probability.clamp(0.0, 1.0)),
            None => false,
        }
    })
}

/// Core buggify check: always false in production (no simulation feature).
#[cfg(not(feature = "simulation"))]
#[inline(always)]
pub fn buggify_check(_probability: f64, _fault_id: &'static str) -> bool {
    false
}

/// The `buggify!` macro.
///
/// First argument: fault model entry ID (must be registered).
/// Second argument: probability (0.0 to 1.0). Defaults to 0.01 if omitted.
///
/// In production builds (without `simulation` feature), this always evaluates
/// to `false` and the fault-injection branch is dead code eliminated.
#[macro_export]
macro_rules! buggify {
    ($fault_id:expr, $probability:expr) => {
        $crate::buggify::buggify_check($probability, $fault_id)
    };
    ($fault_id:expr) => {
        $crate::buggify::buggify_check(0.01, $fault_id)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn buggify_disabled_by_default() {
        assert!(!buggify_enabled());
        assert!(!buggify_check(1.0, "test_fault"));
    }

    #[test]
    fn buggify_init_enables() {
        buggify_init(42);
        assert!(buggify_enabled());
        buggify_disable();
        assert!(!buggify_enabled());
    }

    #[test]
    fn buggify_deterministic_with_same_seed() {
        buggify_init(12345);
        let results1: Vec<bool> = (0..100).map(|_| buggify_check(0.5, "test_fault")).collect();
        buggify_disable();

        buggify_init(12345);
        let results2: Vec<bool> = (0..100).map(|_| buggify_check(0.5, "test_fault")).collect();
        buggify_disable();

        assert_eq!(results1, results2, "Same seed must produce same results");
    }

    #[test]
    fn buggify_different_seed_differs() {
        buggify_init(111);
        let results1: Vec<bool> = (0..100)
            .map(|_| {
                // Use the RNG directly to prove seeds differ
                super::BUGGIFY_RNG.with(|rng| rng.borrow_mut().as_mut().unwrap().gen_bool(0.5))
            })
            .collect();
        buggify_disable();

        buggify_init(222);
        let results2: Vec<bool> = (0..100)
            .map(|_| {
                super::BUGGIFY_RNG.with(|rng| rng.borrow_mut().as_mut().unwrap().gen_bool(0.5))
            })
            .collect();
        buggify_disable();

        assert_ne!(
            results1, results2,
            "Different seeds must produce different results"
        );
    }

    #[test]
    fn buggify_macro_compiles() {
        // Without simulation feature, this should always be false
        let result = buggify!("test_macro_fault", 0.5);
        // In test builds without `simulation` feature, always false
        #[cfg(not(feature = "simulation"))]
        assert!(!result);
        #[cfg(feature = "simulation")]
        let _ = result;
    }

    #[test]
    fn buggify_macro_default_probability() {
        let result = buggify!("test_macro_default");
        #[cfg(not(feature = "simulation"))]
        assert!(!result);
        #[cfg(feature = "simulation")]
        let _ = result;
    }
}
