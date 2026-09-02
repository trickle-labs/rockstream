//! Systemd Service Unit Hardening & Watchdog Configuration Tests (v0.59.22 Slice 3 / Phase 3a).

use std::fs;
use std::path::Path;

#[test]
fn test_systemd_unit_hardening_and_watchdog_config() {
    let systemd_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/systemd");

    let units = [
        "rockstream.service",
        "rockstream-gateway.service",
        "rockstream-worker.service",
        "rockstream-control.service",
    ];

    for unit_name in &units {
        let unit_path = systemd_dir.join(unit_name);
        let content = fs::read_to_string(&unit_path)
            .unwrap_or_else(|_| panic!("systemd unit {unit_name} must exist"));

        // 1. Service User and Group
        assert!(
            content.contains("User=rockstream"),
            "{unit_name} must set User=rockstream"
        );
        assert!(
            content.contains("Group=rockstream"),
            "{unit_name} must set Group=rockstream"
        );

        // 2. Linux Sandboxing & Hardening Directives
        assert!(
            content.contains("ProtectSystem=strict"),
            "{unit_name} must set ProtectSystem=strict"
        );
        assert!(
            content.contains("ProtectHome=yes"),
            "{unit_name} must set ProtectHome=yes"
        );
        assert!(
            content.contains("NoNewPrivileges=yes"),
            "{unit_name} must set NoNewPrivileges=yes"
        );
        assert!(
            content.contains("PrivateTmp=yes"),
            "{unit_name} must set PrivateTmp=yes"
        );
        assert!(
            content.contains("MemoryDenyWriteExecute=yes"),
            "{unit_name} must set MemoryDenyWriteExecute=yes"
        );

        // 3. Resource Limits
        assert!(
            content.contains("LimitNOFILE=65536"),
            "{unit_name} must set LimitNOFILE=65536"
        );

        // 4. Watchdog & Restart Policies
        assert!(
            content.contains("WatchdogSec=30"),
            "{unit_name} must configure WatchdogSec=30"
        );
        assert!(
            content.contains("Restart=on-failure"),
            "{unit_name} must set Restart=on-failure"
        );
    }
}
