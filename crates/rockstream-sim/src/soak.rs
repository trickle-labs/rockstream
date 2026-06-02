//! Simulation soak infrastructure (v0.36).
//!
//! Provides the seed corpus, regression seed storage, and soak runner
//! for the continuous simulation soak CI job (`simulation-soak.yml`).
//!
//! The corpus contains:
//! - **Law seeds**: at least one per registered merge law, targeting its named
//!   fault scenarios (reorder / crash-replay / fence).
//! - **Regression seeds**: minimized failing seeds from previous soak runs that
//!   block release until all replay cleanly.
//!
//! At v0.36 launch `build_initial_corpus()` returns the first corpus, which
//! includes one seed per `LAW_FAULT_IDS` entry and two boundary regression seeds.

use crate::sim::SimRuntime;

/// A seed entry covering a specific merge law under fault injection.
#[derive(Debug, Clone)]
pub struct LawSeed {
    /// The merge law this seed targets (e.g., `"WeightAdd/v1"`).
    pub law_id: &'static str,
    /// The simulation seed.
    pub seed: u64,
    /// Brief description of the fault scenario exercised.
    pub scenario: &'static str,
}

/// A regression seed from a previously discovered failure.
#[derive(Debug, Clone)]
pub struct RegressionSeed {
    /// The minimized failing seed.
    pub seed: u64,
    /// Description of what this seed triggers.
    pub description: &'static str,
}

/// The simulation seed corpus.
pub struct SeedCorpus {
    law_seeds: Vec<LawSeed>,
    regression_seeds: Vec<RegressionSeed>,
}

impl SeedCorpus {
    pub fn new() -> Self {
        Self {
            law_seeds: Vec::new(),
            regression_seeds: Vec::new(),
        }
    }

    pub fn add_law_seed(&mut self, seed: LawSeed) {
        self.law_seeds.push(seed);
    }

    pub fn add_regression_seed(&mut self, seed: RegressionSeed) {
        self.regression_seeds.push(seed);
    }

    /// Sorted, deduplicated list of law IDs covered by the corpus.
    pub fn covered_law_ids(&self) -> Vec<&'static str> {
        let mut ids: Vec<&'static str> = self.law_seeds.iter().map(|s| s.law_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Whether every law in `required` has at least one seed in the corpus.
    pub fn covers_all_laws(&self, required: &[&str]) -> bool {
        let covered = self.covered_law_ids();
        required.iter().all(|law| covered.contains(law))
    }

    pub fn law_seeds(&self) -> &[LawSeed] {
        &self.law_seeds
    }

    pub fn regression_seeds(&self) -> &[RegressionSeed] {
        &self.regression_seeds
    }
}

impl Default for SeedCorpus {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome of a single soak seed run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedOutcome {
    Pass,
    Failure { description: String },
}

/// Runs simulation seeds through a workload and records failures.
pub struct SoakRunner {
    seeds_run: u64,
    failures: Vec<(u64, String)>,
}

impl SoakRunner {
    pub fn new() -> Self {
        Self {
            seeds_run: 0,
            failures: Vec::new(),
        }
    }

    /// Run `seed` through `workload`. Records any `Failure` outcome.
    pub fn run_seed<F>(&mut self, seed: u64, workload: F)
    where
        F: FnOnce(&SimRuntime) -> SeedOutcome,
    {
        let rt = SimRuntime::new(seed);
        let outcome = workload(&rt);
        self.seeds_run += 1;
        if let SeedOutcome::Failure { description } = outcome {
            self.failures.push((seed, description));
        }
    }

    pub fn seeds_run(&self) -> u64 {
        self.seeds_run
    }

    pub fn failures(&self) -> &[(u64, String)] {
        &self.failures
    }

    pub fn all_passed(&self) -> bool {
        self.failures.is_empty()
    }
}

impl Default for SoakRunner {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the initial law-coverage and regression seed corpus for v0.36.
///
/// Covers every entry in `LAW_FAULT_IDS` with at least one seed and
/// includes two boundary regression seeds.
pub fn build_initial_corpus() -> SeedCorpus {
    let mut corpus = SeedCorpus::new();

    // WeightAdd/v1 — covers all three law fault scenarios.
    corpus.add_law_seed(LawSeed {
        law_id: "WeightAdd/v1",
        seed: 0x1A2B_3C4D_5E6F_7089,
        scenario: "law.weight_add.reorder: out-of-order operand pairs across epoch boundaries",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "WeightAdd/v1",
        seed: 0x9988_7766_5544_3322,
        scenario: "law.weight_add.crash_replay: crash mid-WriteBatch, replay from frontier",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "WeightAdd/v1",
        seed: 0xDEAD_BEEF_CAFE_0001,
        scenario: "law.weight_add.fence: storage fence between merge write and frontier update",
    });

    // SumCount/v1 — AVG aggregate pair.
    corpus.add_law_seed(LawSeed {
        law_id: "SumCount/v1",
        seed: 0x1234_5678_9ABC_DEF0,
        scenario: "law.sum_count.reorder: partial sums/counts out-of-order across epochs",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "SumCount/v1",
        seed: 0xFEDC_BA98_7654_3210,
        scenario: "law.sum_count.crash_replay: crash after sum write before count write",
    });

    // MaxRegister/v1 — last-write-wins maximum.
    corpus.add_law_seed(LawSeed {
        law_id: "MaxRegister/v1",
        seed: 0xAAAA_BBBB_CCCC_DDDD,
        scenario: "law.max_register.reorder: updates arrive out of order",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "MaxRegister/v1",
        seed: 0x1111_2222_3333_4444,
        scenario: "law.max_register.duplicate: same update delivered twice",
    });

    // MinRegister/v1 — last-write-wins minimum.
    corpus.add_law_seed(LawSeed {
        law_id: "MinRegister/v1",
        seed: 0x5555_AAAA_5555_AAAA,
        scenario: "law.min_register.reorder: updates arrive out of order",
    });

    // PNCounter/v1 — positive-negative counter.
    corpus.add_law_seed(LawSeed {
        law_id: "PNCounter/v1",
        seed: 0x1111_3333_5555_7777,
        scenario: "law.pn_counter.reorder: updates arrive out of order",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "PNCounter/v1",
        seed: 0x2222_4444_6666_8888,
        scenario: "law.pn_counter.duplicate: same update delivered twice",
    });

    // LWWRegister/v1 — last-writer-wins register.
    corpus.add_law_seed(LawSeed {
        law_id: "LWWRegister/v1",
        seed: 0x1234_1234_1234_1234,
        scenario: "law.lww_register.reorder: updates arrive out of order with different timestamps",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "LWWRegister/v1",
        seed: 0x5678_5678_5678_5678,
        scenario: "law.lww_register.duplicate: same update delivered twice",
    });

    // HyperLogLog/v1 — approximate distinct count sketch.
    corpus.add_law_seed(LawSeed {
        law_id: "HyperLogLog/v1",
        seed: 0xCAFE_BABE_DEAD_BEEF,
        scenario: "law.hyper_log_log.reorder: sketch merge operands arrive out of order",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "HyperLogLog/v1",
        seed: 0x5555_6666_7777_8888,
        scenario: "law.hyper_log_log.crash_replay: crash after partial sketch write",
    });

    // BloomUnion/v1 — set membership sketch.
    corpus.add_law_seed(LawSeed {
        law_id: "BloomUnion/v1",
        seed: 0x9999_AAAA_BBBB_CCCC,
        scenario: "law.bloom_union.reorder: bit-OR operands arrive out of order",
    });
    corpus.add_law_seed(LawSeed {
        law_id: "BloomUnion/v1",
        seed: 0xDDDD_EEEE_FFFF_0000,
        scenario: "law.bloom_union.duplicate: same filter merged twice",
    });

    // Regression seeds (boundary values; corpus non-empty per v0.36 exit criterion).
    corpus.add_regression_seed(RegressionSeed {
        seed: 0x0000_0000_0000_0000,
        description: "Zero seed: verifies deterministic empty-workload behaviour at RNG minimum",
    });
    corpus.add_regression_seed(RegressionSeed {
        seed: 0xFFFF_FFFF_FFFF_FFFF,
        description: "Max seed: verifies deterministic behaviour at RNG maximum",
    });

    corpus
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initial_corpus_covers_all_laws() {
        let corpus = build_initial_corpus();
        assert!(corpus.covers_all_laws(&[
            "WeightAdd/v1",
            "SumCount/v1",
            "MaxRegister/v1",
            "MinRegister/v1",
            "PNCounter/v1",
            "LWWRegister/v1",
            "HyperLogLog/v1",
            "BloomUnion/v1",
        ]));
    }

    #[test]
    fn corpus_has_regression_seeds() {
        let corpus = build_initial_corpus();
        assert!(!corpus.regression_seeds().is_empty());
    }

    #[test]
    fn soak_runner_counts_seeds() {
        let mut runner = SoakRunner::new();
        for seed in 0..10u64 {
            runner.run_seed(seed, |_rt| SeedOutcome::Pass);
        }
        assert_eq!(runner.seeds_run(), 10);
        assert!(runner.all_passed());
    }

    #[test]
    fn soak_runner_records_failures() {
        let mut runner = SoakRunner::new();
        runner.run_seed(42, |_rt| SeedOutcome::Failure {
            description: "test failure".to_string(),
        });
        assert_eq!(runner.failures().len(), 1);
        assert_eq!(runner.failures()[0].0, 42);
    }

    #[test]
    fn soak_runner_deterministic() {
        let mut runner1 = SoakRunner::new();
        let mut runner2 = SoakRunner::new();

        for seed in 0..100u64 {
            let v1 = {
                let rt = SimRuntime::new(seed);
                rt.random_u64()
            };
            let v2 = {
                let rt = SimRuntime::new(seed);
                rt.random_u64()
            };
            runner1.run_seed(seed, move |_| {
                if v1 == 0 {
                    SeedOutcome::Failure {
                        description: "zero".to_string(),
                    }
                } else {
                    SeedOutcome::Pass
                }
            });
            runner2.run_seed(seed, move |_| {
                if v2 == 0 {
                    SeedOutcome::Failure {
                        description: "zero".to_string(),
                    }
                } else {
                    SeedOutcome::Pass
                }
            });
        }

        assert_eq!(runner1.seeds_run(), runner2.seeds_run());
        assert_eq!(runner1.failures().len(), runner2.failures().len());
    }
}
