use rockstream_sim::buggify::{buggify_disable, buggify_focus, buggify_init};
use rockstream_sim::SimRuntime;
use rockstream_types::compatibility::{
    ProtocolVersion, StorageFormatVersion, SupportedStorageFormatRange, SupportedVersionRange,
};
use rockstream_types::ids::WorkerId;
use rockstream_types::topology::{
    assignment_compatible, CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo,
    WorkerLifecycleState, WorkerLocation,
};

fn worker(id: u64, max: u32) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(id),
        role: NodeRole::Worker,
        address: format!("127.0.0.1:{}", 8000 + id),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::default(),
        capabilities: WorkerCapabilities::default(),
        protocol_range: SupportedVersionRange::new(ProtocolVersion::V1, ProtocolVersion(max)),
        storage_format_range: SupportedStorageFormatRange::new(
            StorageFormatVersion::V1,
            StorageFormatVersion(max as u8),
        ),
        registered_at_ms: 1,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

fn assignment_events(workers: &[WorkerInfo]) -> Vec<String> {
    let _runtime = SimRuntime::new(0x56_0001);
    buggify_init(0x56_0001);
    let mut events = Vec::new();
    buggify_focus("upgrade.before_assignment_compatibility_check");
    if rockstream_sim::buggify!("upgrade.before_assignment_compatibility_check", 1.0) {
        events.push("compatibility_check=started".to_string());
    }
    if assignment_compatible(workers, ProtocolVersion::V2, StorageFormatVersion::V2) {
        events.push("assignment=granted".to_string());
    } else {
        events.push("assignment=withheld".to_string());
    }
    buggify_focus("upgrade.after_worker_restart_before_reassign");
    if rockstream_sim::buggify!("upgrade.after_worker_restart_before_reassign", 1.0) {
        events.push("restart_boundary=checked".to_string());
    }
    events.extend(["epoch=1", "epoch=2"].into_iter().map(str::to_string));
    buggify_disable();
    events
}

#[test]
fn mixed_versions_withhold_cross_version_assignment_seeded() {
    let events = assignment_events(&[worker(1, 1), worker(2, 2), worker(3, 2)]);
    assert_eq!(
        events,
        vec![
            "compatibility_check=started",
            "assignment=withheld",
            "restart_boundary=checked",
            "epoch=1",
            "epoch=2",
        ]
    );
}

#[test]
fn mixed_versions_assign_after_compatibility_floor_seeded() {
    let events = assignment_events(&[worker(1, 2), worker(2, 2), worker(3, 2)]);
    assert_eq!(
        events,
        vec![
            "compatibility_check=started",
            "assignment=granted",
            "restart_boundary=checked",
            "epoch=1",
            "epoch=2",
        ]
    );
}
