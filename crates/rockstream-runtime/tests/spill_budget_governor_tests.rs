//! Worker Budget Integration & Spill Governors Tests (v0.67.1 Slice 6 / Phase 3b).
//!
//! Validates:
//! 1. `test_spill_write_buffer_halts_sender_at_limit`: Spill write buffer bounds in-flight memory.
//! 2. `test_disk_occupancy_ceiling_rejects_with_rs2021`: Disk ceiling rejects writes with RS-2021.
//! 3. `test_concurrent_spill_io_governed_to_configured_bound`: Concurrency limits bound active tasks.
//! 4. `test_spill_pressure_pauses_and_resumes_sources`: 80%/95%/75% hysteresis pauses/resumes sources.
//! 5. `test_foreground_queries_succeed_during_heavy_spill`: Foreground reservation is preserved.

use std::sync::Arc;

use rockstream_runtime::source_pressure::SourcePressureState;
use rockstream_runtime::spill_governor::{SpillGovernor, SpillGovernorConfig};
use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};

#[test]
fn test_spill_write_buffer_halts_sender_at_limit() {
    let budget_bytes = 64 * 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let config = SpillGovernorConfig {
        max_spill_write_buffer_bytes: 16 * 1024 * 1024, // 16 MiB limit
        max_disk_occupancy_bytes: 10 * 1024 * 1024 * 1024,
        max_concurrent_spill_io: 4,
        foreground_reservation_bytes: 0,
    };
    let governor = SpillGovernor::new(ledger.clone(), config);

    // Write 16 MiB -> succeeds
    governor
        .record_spill_write(16 * 1024 * 1024)
        .expect("16 MiB write succeeds");
    assert_eq!(governor.current_write_buffer_bytes(), 16 * 1024 * 1024);

    // Next write exceeds 16 MiB buffer bound -> must halt sender / reject
    let err = governor
        .record_spill_write(1024)
        .expect_err("exceeding buffer limit must fail");
    assert!(err.to_string().contains("RS-5003"));
    assert!(err.to_string().contains("spill-write-buffer"));

    // Flush 8 MiB to disk
    governor.flush_spill_write_buffer(8 * 1024 * 1024);
    assert_eq!(governor.current_write_buffer_bytes(), 8 * 1024 * 1024);
    assert_eq!(governor.current_disk_occupancy(), 8 * 1024 * 1024);

    // Now writing 8 MiB succeeds again
    governor
        .record_spill_write(8 * 1024 * 1024)
        .expect("write succeeds after flush");
    assert_eq!(governor.current_write_buffer_bytes(), 16 * 1024 * 1024);
}

#[test]
fn test_disk_occupancy_ceiling_rejects_with_rs2021() {
    let budget_bytes = 64 * 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let config = SpillGovernorConfig {
        max_spill_write_buffer_bytes: 16 * 1024 * 1024,
        max_disk_occupancy_bytes: 50 * 1024 * 1024, // 50 MiB disk ceiling
        max_concurrent_spill_io: 4,
        foreground_reservation_bytes: 0,
    };
    let governor = SpillGovernor::new(ledger, config);

    // Write and flush 50 MiB in batches
    for _ in 0..5 {
        governor
            .record_spill_write(10 * 1024 * 1024)
            .expect("record write");
        governor.flush_spill_write_buffer(10 * 1024 * 1024);
    }
    assert_eq!(governor.current_disk_occupancy(), 50 * 1024 * 1024);

    // Further write exceeds disk ceiling -> rejected with RS-2021
    let err = governor
        .record_spill_write(1024)
        .expect_err("disk ceiling must reject write");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("RS-2021"),
        "expected RS-2021 in error message, got: {err_msg}"
    );
    assert!(err_msg.contains("disk occupancy ceiling exceeded"));
}

#[test]
fn test_concurrent_spill_io_governed_to_configured_bound() {
    let budget_bytes = 64 * 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let config = SpillGovernorConfig {
        max_spill_write_buffer_bytes: 16 * 1024 * 1024,
        max_disk_occupancy_bytes: 50 * 1024 * 1024 * 1024,
        max_concurrent_spill_io: 4,
        foreground_reservation_bytes: 0,
    };
    let governor = SpillGovernor::new(ledger, config);

    // Acquire 4 permits
    let p1 = governor.try_acquire_spill_io().expect("permit 1");
    let p2 = governor.try_acquire_spill_io().expect("permit 2");
    let p3 = governor.try_acquire_spill_io().expect("permit 3");
    let p4 = governor.try_acquire_spill_io().expect("permit 4");

    // 5th attempt must fail with concurrency limit error
    let err = governor
        .try_acquire_spill_io()
        .expect_err("5th permit must exceed limit");
    assert!(err.to_string().contains("RS-9001"));

    // Drop permit 1 -> can acquire again
    drop(p1);
    let p5 = governor
        .try_acquire_spill_io()
        .expect("permit after release");
    drop((p2, p3, p4, p5));
}

#[test]
fn test_spill_pressure_pauses_and_resumes_sources() {
    let budget_bytes = 100 * 1024 * 1024; // 100 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let config = SpillGovernorConfig::default();
    let governor = SpillGovernor::new(ledger.clone(), config);

    let sp = governor.source_pressure();
    assert_eq!(sp.pressure_state(), SourcePressureState::Normal);
    assert_eq!(sp.available_credits(), 100);
    assert!(sp.can_ingest().is_ok());

    // Allocate 80 MiB (gross ~88 MiB with 10% overhead => 88%)
    let permit1 = ledger
        .try_acquire(MemoryCategory::OperatorState, 80 * 1024 * 1024, false)
        .expect("acquire 80 MiB");
    assert_eq!(sp.pressure_state(), SourcePressureState::Throttled);
    assert_eq!(sp.available_credits(), 50); // Cut to 50%
    assert!(sp.can_ingest().is_ok());

    // Allocate 8 MiB more (total 88 MiB gross ~96.8 MiB => >95%)
    let permit2 = ledger
        .try_acquire(MemoryCategory::OperatorState, 8 * 1024 * 1024, false)
        .expect("acquire 8 MiB");
    assert_eq!(sp.pressure_state(), SourcePressureState::Paused);
    assert_eq!(sp.available_credits(), 0);
    let ingest_err = sp.can_ingest().expect_err("paused must reject ingest");
    assert!(ingest_err.to_string().contains("RS-5003"));

    // Release memory down to 50 MiB (< 75%) -> resumes to Normal
    drop(permit2);
    ledger.release(MemoryCategory::OperatorState, 30 * 1024 * 1024);
    assert_eq!(sp.pressure_state(), SourcePressureState::Normal);
    assert_eq!(sp.available_credits(), 100);
    assert!(sp.can_ingest().is_ok());
    drop(permit1);
}

#[test]
fn test_foreground_queries_succeed_during_heavy_spill() {
    let budget_bytes = 100 * 1024 * 1024; // 100 MiB
    let foreground_reservation = 20 * 1024 * 1024; // 20 MiB dedicated to foreground
    let ledger = Arc::new(WorkerBudgetLedger::new(
        budget_bytes,
        foreground_reservation,
    ));
    let config = SpillGovernorConfig {
        max_spill_write_buffer_bytes: 80 * 1024 * 1024,
        max_disk_occupancy_bytes: 50 * 1024 * 1024 * 1024,
        max_concurrent_spill_io: 4,
        foreground_reservation_bytes: foreground_reservation as u64,
    };
    let governor = SpillGovernor::new(ledger.clone(), config);

    // Heavy background spill allocates 70 MiB (gross with 10% overhead = 77 MiB out of 80 MiB non-reserved)
    governor
        .record_spill_write(70 * 1024 * 1024)
        .expect("spill writes within background budget");

    // Further background allocation exceeding background capacity (80 MiB) is rejected
    let bg_err = governor.record_spill_write(10 * 1024 * 1024);
    assert!(bg_err.is_err());

    // Foreground query requesting 10 MiB succeeds because foreground reservation is protected
    let permit = governor
        .admit_foreground_query(10 * 1024 * 1024)
        .expect("foreground query admitted from reserved capacity");
    assert_eq!(permit.bytes(), 10 * 1024 * 1024);
    assert_eq!(permit.category(), MemoryCategory::QueryWorkMemory);

    // Drop permit releases memory
    drop(permit);
}
