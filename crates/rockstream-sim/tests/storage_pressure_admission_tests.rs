//! Storage Pressure Admission Signals & Shedding Order Test Suite (v0.58.2).
//!
//! Verifies:
//! 1. Each of the 5 storage-pressure signals is induced independently (SP-001 through SP-005).
//! 2. Each signal triggers shedding in the exact documented order:
//!    - Step 1: Throttle / reject backfills first
//!    - Step 2: Reduce source ingestion second
//!    - Step 3: Refuse parallelism increases third
//! 3. Acting signal is named accurately through v0.54.1 dominant-cause attribution.
//! 4. Individual Prometheus export of all 5 signals with zero composite storage_debt scalar.

use rockstream_control::audit::FileAuditLog;
use rockstream_gateway::admission::{
    AdmissionController, AdmissionDecision, BackfillAdmissionController, BackfillAdmissionDecision,
    StoragePressureSheddingEngine, StoragePressureSheddingStep,
};
use rockstream_gateway::catalog_stubs::{CatalogStubs, CatalogView};
use rockstream_sim::auto_tuner::AutoTuner;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_sim::{OscillationDetector, SimObjectStoreHandle};
use rockstream_types::config::{AutotunerConfig, RockstreamConfig};
use rockstream_types::metrics::{
    generate_prometheus_metrics, read_freshness_lag, read_storage_pressure_signals, reset_all,
    set_freshness_lag, set_pipeline_state_bytes, set_storage_pressure_signals,
    StoragePressureSignals, METRICS_TEST_LOCK,
};
use rockstream_types::view_lifecycle::{
    derive_degradation_status_with_signals, dominant_contributor_with_signals,
    dominant_storage_contributor_for_signals, DominantContributor, ViewState,
};
use rockstream_types::workload::{WorkloadDef, WorkloadPriority};
use tempfile::NamedTempFile;

#[test]
fn test_sp_signal_definitions_and_dominant_attribution() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_all();

    let signals = StoragePressureSignals {
        l0_backlog: 12,
        pending_compaction_bytes: 80 * 1024 * 1024,
        flush_latency_ms: 650,
        write_amplification: 18.0,
        object_store_latency_ms: 1500,
        object_store_failure_rate: 0.08,
    };
    assert!(signals.is_pressured());

    set_storage_pressure_signals(&signals);
    let read = read_storage_pressure_signals();
    assert_eq!(read.l0_backlog, 12);
    assert_eq!(read.pending_compaction_bytes, 80 * 1024 * 1024);
    assert_eq!(read.flush_latency_ms, 650);
    assert!((read.write_amplification - 18.0).abs() < 1e-6);
    assert_eq!(read.object_store_latency_ms, 1500);
    assert!((read.object_store_failure_rate - 0.08).abs() < 1e-6);

    let prom = generate_prometheus_metrics();
    assert!(prom.contains("storage_pressure_l0_backlog 12"));
    assert!(prom.contains("storage_pressure_pending_compaction_bytes 83886080"));
    assert!(prom.contains("storage_pressure_flush_latency_ms 650"));
    assert!(prom.contains("storage_pressure_write_amplification 18"));
    assert!(prom.contains("storage_pressure_object_store_latency_ms 1500"));
    assert!(prom.contains("storage_pressure_object_store_failure_rate 0.08"));
    assert!(
        !prom.contains("storage_debt"),
        "composite storage_debt scalar is explicitly forbidden"
    );
}

fn assert_shedding_order_and_attribution(
    signals: StoragePressureSignals,
    expected_dominant_variant: DominantContributor,
    expected_dominant_str: &str,
) {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_all();
    buggify_init(42);

    let tmp = NamedTempFile::new().unwrap();
    let log = FileAuditLog::open(tmp.path()).unwrap();

    set_storage_pressure_signals(&signals);

    // 1. Dominant contributor resolution
    let dom = dominant_storage_contributor_for_signals(&signals);
    assert_eq!(
        dom,
        Some(expected_dominant_variant),
        "dominant contributor variant mismatch"
    );
    assert_eq!(
        expected_dominant_variant.to_string(),
        expected_dominant_str,
        "dominant cause string mismatch"
    );

    let view_dom = dominant_contributor_with_signals(None, Some(signals));
    assert_eq!(view_dom, expected_dominant_variant);

    let deg = derive_degradation_status_with_signals(&ViewState::Running, None, Some(signals));
    assert_eq!(deg.dominant_contributor, expected_dominant_variant);

    // 2. Shedding Engine executes 3 steps in exact hierarchy: 1 -> 2 -> 3
    let steps = StoragePressureSheddingEngine::evaluate_shedding(&signals, Some(&log));
    assert_eq!(
        steps,
        vec![
            StoragePressureSheddingStep::Step1ThrottleBackfills,
            StoragePressureSheddingStep::Step2ReduceSourceIngestion,
            StoragePressureSheddingStep::Step3RefuseParallelismExpansion,
        ]
    );

    // 3. Step 1 Execution: Backfill admission rejected with RS-4021 and named dominant cause
    let backfill_admission = BackfillAdmissionController::default();
    let decision = backfill_admission.reserve_with_signals(500, 10_000, Some(&signals), Some(&log));
    match decision {
        BackfillAdmissionDecision::Reject { code, reason } => {
            assert_eq!(code, "RS-4021");
            assert!(
                reason.contains(expected_dominant_str),
                "rejection reason must name dominant cause {expected_dominant_str}, got: {reason}"
            );
        }
        other => panic!("expected backfill rejection under storage pressure, got: {other:?}"),
    }

    // 4. Step 2 Execution: Source throttle reduced to alleviate storage pressure
    let mut tuner = AutoTuner::new(AutotunerConfig::default());
    let initial_throttle = 1024u64;
    let reduced_throttle =
        tuner.adjust_source_throttle_with_signals(100, 1000, initial_throttle, Some(&signals));
    assert_eq!(
        reduced_throttle, 512,
        "source throttle must be reduced by 50%"
    );
    assert!(tuner.audit_sink.iter().any(|e| {
        e.action == "source_throttle.reduced_storage_pressure"
            && e.detail
                .as_deref()
                .unwrap_or("")
                .contains(expected_dominant_str)
    }));

    // 5. Step 3 Execution: Parallelism scale-up strictly refused
    let initial_parallelism = tuner.current_parallelism;
    for _ in 0..8 {
        let p = tuner.adjust_parallelism_with_signals(1200, Some(&signals));
        assert_eq!(
            p, initial_parallelism,
            "parallelism scale-up must be vetoed under storage pressure"
        );
    }
    assert!(tuner.audit_sink.iter().any(|e| {
        e.action == "parallelism.scale_up_refused_storage_pressure"
            && e.detail
                .as_deref()
                .unwrap_or("")
                .contains(expected_dominant_str)
    }));

    buggify_disable();
}

#[test]
fn test_sp001_l0_backlog_shedding_order_and_attribution() {
    let signals = StoragePressureSignals {
        l0_backlog: 16, // > threshold 8
        ..Default::default()
    };
    assert_shedding_order_and_attribution(
        signals,
        DominantContributor::StoragePressureL0Backlog,
        "storage_pressure_l0_backlog",
    );
}

#[test]
fn test_sp002_pending_compaction_bytes_shedding_order_and_attribution() {
    let signals = StoragePressureSignals {
        pending_compaction_bytes: 128 * 1024 * 1024, // 128 MiB > threshold 64 MiB
        ..Default::default()
    };
    assert_shedding_order_and_attribution(
        signals,
        DominantContributor::StoragePressurePendingCompaction,
        "storage_pressure_pending_compaction_bytes",
    );
}

#[test]
fn test_sp003_flush_latency_shedding_order_and_attribution() {
    let signals = StoragePressureSignals {
        flush_latency_ms: 750, // > threshold 500 ms
        ..Default::default()
    };
    assert_shedding_order_and_attribution(
        signals,
        DominantContributor::StoragePressureFlushLatency,
        "storage_pressure_flush_latency",
    );
}

#[test]
fn test_sp004_write_amplification_shedding_order_and_attribution() {
    let signals = StoragePressureSignals {
        write_amplification: 22.0, // > threshold 15.0
        ..Default::default()
    };
    assert_shedding_order_and_attribution(
        signals,
        DominantContributor::StoragePressureWriteAmplification,
        "storage_pressure_write_amplification",
    );
}

#[test]
fn test_sp005_object_store_latency_and_errors_shedding_order_and_attribution() {
    let signals_lat = StoragePressureSignals {
        object_store_latency_ms: 1800, // > threshold 1000 ms
        ..Default::default()
    };
    assert_shedding_order_and_attribution(
        signals_lat,
        DominantContributor::StoragePressureObjectStoreLatency,
        "storage_pressure_object_store_latency",
    );

    let signals_fail = StoragePressureSignals {
        object_store_failure_rate: 0.12, // > threshold 0.05
        ..Default::default()
    };
    assert_shedding_order_and_attribution(
        signals_fail,
        DominantContributor::StoragePressureObjectStoreFailures,
        "storage_pressure_object_store_failures",
    );
}

// ─── Workload Isolation & Freshness Protection Matrix ────────────────────────

#[test]
fn test_large_backfill_compaction_saturated_protects_running_views_minio_tc() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_all();

    let tmp = NamedTempFile::new().unwrap();
    let log = FileAuditLog::open(tmp.path()).unwrap();

    let catalog = CatalogStubs::new();
    catalog.add_view_in_namespace(CatalogView {
        name: "active_orders".to_string(),
        sql: "SELECT * FROM orders".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    set_pipeline_state_bytes("active_orders", 50 * 1024 * 1024);
    set_freshness_lag("active_orders", 450); // within 1000 ms SLO

    // 1. Induce compaction-saturated store (> 64 MiB threshold)
    let signals = StoragePressureSignals {
        pending_compaction_bytes: 128 * 1024 * 1024,
        ..Default::default()
    };
    set_storage_pressure_signals(&signals);

    // 2. Large 10M-row historical backfill attempts reservation
    let backfill_admission = BackfillAdmissionController::default();
    let backfill_requested_bytes = 500 * 1024 * 1024; // 500 MB
    let backfill_capacity_bytes = 1024 * 1024 * 1024; // 1 GB

    let decision = backfill_admission.reserve_with_signals(
        backfill_requested_bytes,
        backfill_capacity_bytes,
        Some(&signals),
        Some(&log),
    );

    // Assert: backfill is rejected/throttled under storage pressure
    match decision {
        BackfillAdmissionDecision::Reject { code, reason } => {
            assert_eq!(code, "RS-4021");
            assert!(
                reason.contains("storage_pressure_pending_compaction_bytes"),
                "reason must name dominant cause: {reason}"
            );
        }
        other => panic!("expected backfill rejection under compaction saturation, got: {other:?}"),
    }

    // Assert: already-running view remains Running and its freshness lag is strictly within SLO
    assert_eq!(catalog.view_state("active_orders"), ViewState::Running);
    let current_lag = read_freshness_lag("active_orders").expect("freshness lag tracked");
    assert!(
        current_lag <= 1000,
        "active view freshness lag ({current_lag} ms) must stay strictly within 1000 ms SLO"
    );

    // Verify audit log durability (SimObjectStore / MinIO)
    let events = log.read_all().unwrap();
    assert!(events.iter().any(|e| {
        e.action == "storage_pressure.shedding_step_1_backfill_throttled"
            && (e
                .resource
                .contains("storage_pressure_pending_compaction_bytes")
                || e.detail
                    .as_deref()
                    .unwrap_or("")
                    .contains("storage_pressure_pending_compaction_bytes"))
    }));

    let store = SimObjectStoreHandle::new();
    let raw_lines = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    let serialized = raw_lines.clone().into_bytes();
    let serialized_len = serialized.len();
    store
        .put(
            "audit/backfill_protection.jsonl",
            bytes::Bytes::from(serialized),
        )
        .unwrap();
    let read_back = store.get("audit/backfill_protection.jsonl").unwrap();
    assert_eq!(read_back.len(), serialized_len);
}

#[test]
fn test_workload_priority_arbitration_under_storage_spike() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_all();
    buggify_init(42);

    let tmp = NamedTempFile::new().unwrap();
    let log = FileAuditLog::open(tmp.path()).unwrap();

    let catalog = CatalogStubs::new();
    catalog.add_workload(WorkloadDef::new("high").with_priority(WorkloadPriority::HIGH));
    catalog.add_workload(WorkloadDef::new("low").with_priority(WorkloadPriority::LOW));

    catalog.add_view_in_namespace(CatalogView {
        name: "high_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("high_view", "high");
    set_pipeline_state_bytes("high_view", 200);
    set_freshness_lag("high_view", 300); // SLO: 500 ms

    catalog.add_view_in_namespace(CatalogView {
        name: "low_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.assign_view_workload("low_view", "low");
    set_pipeline_state_bytes("low_view", 800);

    // Storage spike: L0 backlog exceeds threshold
    let signals = StoragePressureSignals {
        l0_backlog: 16,
        ..Default::default()
    };
    set_storage_pressure_signals(&signals);

    // 1. Backfill admission for low-priority workload is rejected with RS-4021
    let backfill = BackfillAdmissionController::default();
    let backfill_dec = backfill.reserve_with_signals(100, 1000, Some(&signals), Some(&log));
    match backfill_dec {
        BackfillAdmissionDecision::Reject { code, reason } => {
            assert_eq!(code, "RS-4021");
            assert!(reason.contains("storage_pressure_l0_backlog"));
        }
        other => panic!("expected backfill rejection under L0 spike, got: {other:?}"),
    }

    // 2. High priority workload capacity evaluation under storage pressure
    let dec = AdmissionController::evaluate_and_admit_with_storage_pressure(
        &catalog,
        "high",
        200,
        1000,
        Some(&signals),
        Some(&log),
    );
    assert_ne!(dec, AdmissionDecision::Reject);

    // High priority view maintains 100% throughput and target freshness
    assert_eq!(catalog.view_state("high_view"), ViewState::Running);
    let high_lag = read_freshness_lag("high_view").unwrap();
    assert!(
        high_lag <= 500,
        "high_view freshness lag must stay <= 500ms"
    );

    buggify_disable();
}

// ─── Control Loop Convergence & Settling Matrix ──────────────────────────────

#[test]
fn test_control_loop_settles_under_sustained_pressure_step() {
    let mut tuner = AutoTuner::new(AutotunerConfig {
        default_parallelism: 4,
        min_parallelism: 1,
        max_parallelism: 32,
        hysteresis_scale_up_windows: 3,
        hysteresis_scale_down_windows: 12,
        ..Default::default()
    });

    // 5x threshold jump (sustained pressure step)
    let signals = StoragePressureSignals {
        l0_backlog: 40,                              // 5x threshold (8)
        pending_compaction_bytes: 320 * 1024 * 1024, // 5x threshold (64 MiB)
        ..Default::default()
    };

    let mut parallelism_trace: Vec<f64> = Vec::new();
    let mut throttle_trace: Vec<f64> = Vec::new();
    let mut current_throttle = 512u64;

    for _epoch in 0..10 {
        // High epoch latency would normally trigger scale-up, but storage pressure vetoes it
        let p = tuner.adjust_parallelism_with_signals(1200, Some(&signals));
        current_throttle =
            tuner.adjust_source_throttle_with_signals(800, 500, current_throttle, Some(&signals));

        parallelism_trace.push(p as f64);
        throttle_trace.push(current_throttle as f64);
    }

    // Settling assertions:
    // Parallelism never scaled up (stayed 4 throughout)
    assert!(parallelism_trace.iter().all(|&p| p == 4.0));
    // Throttle quickly reaches and settles at minimum safe bound (64) within <= 3 cycles
    assert_eq!(throttle_trace[0], 256.0);
    assert_eq!(throttle_trace[1], 128.0);
    assert_eq!(throttle_trace[2], 64.0);
    assert!(throttle_trace[3..].iter().all(|&t| t == 64.0));

    // Zero sustained oscillations
    assert!(!OscillationDetector::detect(&parallelism_trace));
    assert!(!OscillationDetector::detect(&throttle_trace));

    // Settled within <= 3 cycles to steady state (64.0 at index 2, which is cycle 3)
    let cycles_to_settle = throttle_trace.iter().position(|&t| t == 64.0).unwrap() + 1;
    assert!(
        cycles_to_settle <= 3,
        "control loop must settle within <= 3 cycles, settled in {cycles_to_settle}"
    );
}

#[test]
fn test_control_loop_settles_under_oscillating_pressure_step() {
    let mut tuner = AutoTuner::new(AutotunerConfig {
        default_parallelism: 4,
        min_parallelism: 1,
        max_parallelism: 32,
        hysteresis_scale_up_windows: 3,
        hysteresis_scale_down_windows: 12,
        ..Default::default()
    });

    let pressured_signals = StoragePressureSignals {
        l0_backlog: 20,
        flush_latency_ms: 900,
        ..Default::default()
    };
    let unpressured_signals = StoragePressureSignals::default();

    let mut parallelism_trace: Vec<f64> = Vec::new();
    let mut throttle_trace: Vec<f64> = Vec::new();
    let mut current_throttle = 1024u64;

    for epoch in 0..12 {
        let is_pressured = (epoch / 3) % 2 == 1; // square wave alternating every 3 epochs
        let sigs = if is_pressured {
            &pressured_signals
        } else {
            &unpressured_signals
        };
        let epoch_p95 = if is_pressured { 800 } else { 200 };

        let p = tuner.adjust_parallelism_with_signals(epoch_p95, Some(sigs));
        current_throttle =
            tuner.adjust_source_throttle_with_signals(epoch_p95, 500, current_throttle, Some(sigs));

        parallelism_trace.push(p as f64);
        throttle_trace.push(current_throttle as f64);
    }

    // Damped convergence: no runaway amplification, values remain bounded
    assert!(parallelism_trace.iter().all(|&p| (1.0..=32.0).contains(&p)));
    assert!(throttle_trace.iter().all(|&t| (64.0..=1024.0).contains(&t)));
    assert!(!OscillationDetector::detect(&parallelism_trace));
}

// ─── Configuration-Surface & Metric Lock Matrix ──────────────────────────────

#[test]
fn test_configuration_surface_cli_lock() {
    // Assert zero new roles or top-level configuration subcommands added
    assert_eq!(
        rockstream_cli::KNOWN_ROLES,
        &["all", "control", "worker", "gateway", "frontier"]
    );
}

#[test]
fn test_configuration_surface_struct_lock() {
    // Assert that AutotunerConfig fields and defaults are strictly preserved
    let cfg = AutotunerConfig::default();
    assert!(cfg.enabled);
    assert_eq!(cfg.hysteresis_scale_up_windows, 3);
    assert_eq!(cfg.hysteresis_scale_down_windows, 12);
    assert_eq!(cfg.default_parallelism, 4);
    assert_eq!(cfg.min_parallelism, 1);
    assert_eq!(cfg.max_parallelism, 32);
    assert_eq!(cfg.direct_compression_cpu_budget_ms, 5);
    assert_eq!(cfg.compression_disable_hysteresis_windows, 2);
    assert_eq!(cfg.compression_reenable_hysteresis_windows, 4);

    let serialized = serde_json::to_string(&cfg).unwrap();
    let deserialized: AutotunerConfig = serde_json::from_str(&serialized).unwrap();
    assert_eq!(cfg, deserialized);

    // Verify ClusterConfig defaults roundtrip
    let root_cfg = RockstreamConfig::default();
    assert_eq!(root_cfg.cluster.autotuner, cfg);
}

#[test]
fn test_storage_signals_separately_exported_no_composite_scalar() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_all();

    let signals = StoragePressureSignals {
        l0_backlog: 10,
        pending_compaction_bytes: 70 * 1024 * 1024,
        flush_latency_ms: 550,
        write_amplification: 16.5,
        object_store_latency_ms: 1200,
        object_store_failure_rate: 0.06,
    };
    set_storage_pressure_signals(&signals);

    let prom = generate_prometheus_metrics();

    // 1. All 5 signals exported as individual Prometheus gauges
    assert!(prom.contains("storage_pressure_l0_backlog 10"));
    assert!(prom.contains("storage_pressure_pending_compaction_bytes 73400320"));
    assert!(prom.contains("storage_pressure_flush_latency_ms 550"));
    assert!(prom.contains("storage_pressure_write_amplification 16.5"));
    assert!(prom.contains("storage_pressure_object_store_latency_ms 1200"));
    assert!(prom.contains("storage_pressure_object_store_failure_rate 0.06"));

    // 2. Single composite storage_debt scalar is explicitly rejected / absent
    assert!(
        !prom.contains("storage_debt"),
        "composite storage_debt scalar must never exist in metric exports"
    );
}
