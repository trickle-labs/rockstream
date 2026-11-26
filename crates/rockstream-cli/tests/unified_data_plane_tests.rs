//! v0.51.3 Slice 2 exit test: `--role all` opens exactly one shared data
//! plane (no second, unreferenced `gateway-shard/` directory).
//!
//! `run_start` blocks on an OS shutdown signal while serving the gateway, so
//! this test launches the CLI as a child and sends it a real SIGTERM once the
//! shard layout has settled.

use sha2::Digest;
use std::net::TcpListener;
use std::time::Duration;
use std::{
    io::Read,
    process::{Command, Stdio},
};

#[test]
fn role_all_creates_exactly_one_shard_directory() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();
    let pki = rockstream_test_support::pki::TestPki::generate();
    let mut config = rockstream_types::config::NodeConfig::default();
    config.control.management_addr = Some("127.0.0.1:0".to_owned());
    let internal_tls = pki.worker_tls_config(1);
    let cert_path = internal_tls.cert_path.unwrap();
    let key_path = internal_tls.key_path.unwrap();
    let ca_cert_path = internal_tls.ca_cert_path.unwrap();
    std::fs::write(
        dir.path().join("rockstream.toml"),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(&pki.cli_cert_path).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let administrator = rockstream_types::authorization::SecurityPrincipal {
        format_version: rockstream_types::authorization::SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: rockstream_types::ids::PrincipalId(1),
        display_name: "role all test administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![rockstream_types::authorization::LoginMethod::Mtls {
            certificate_sha256: sha2::Sha256::digest(certs[0].as_ref()).into(),
        }],
        preset_grants: vec![rockstream_types::authorization::ScopedPresetGrant {
            preset: rockstream_types::authorization::PermissionPreset::Administrator,
            scope: rockstream_types::authorization::PolicyScope::Cluster,
        }],
        scoped_grants: Vec::new(),
    };
    let principal_file = dir.path().join("administrator.json");
    std::fs::write(&principal_file, serde_json::to_vec(&administrator).unwrap()).unwrap();
    let bootstrap = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .current_dir(dir.path())
        .arg("--json")
        .arg("security")
        .arg("preprovision-admin")
        .arg("--storage")
        .arg(&storage)
        .arg("--layout")
        .arg("all")
        .arg("--principal-file")
        .arg(&principal_file)
        .arg("--operation-id")
        .arg("1")
        .arg("--yes")
        .output()
        .unwrap();
    assert!(
        bootstrap.status.success(),
        "{}",
        String::from_utf8_lossy(&bootstrap.stderr)
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_addr = listener.local_addr().unwrap();
    drop(listener);
    let mut child = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .current_dir(dir.path())
        .env_remove("ROCKSTREAM_CONFIG")
        .args([
            "start",
            "--storage",
            storage.to_str().unwrap(),
            "--role",
            "all",
            "--auth",
            "off",
            "--allow-insecure-internal-dev",
            "--internal-tls-cert-path",
            cert_path.to_str().unwrap(),
            "--internal-tls-key-path",
            key_path.to_str().unwrap(),
            "--internal-tls-ca-cert-path",
            ca_cert_path.to_str().unwrap(),
            "--listen",
            &listen_addr.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start rockstream child");

    // Wait until the gateway has flushed, bound, and installed its signal
    // handler before inspecting storage or sending SIGTERM.
    let shard0_dir = storage.join("shards").join("0");
    let manifest_dir = shard0_dir.join("db").join("manifest");
    let audit_path = storage.join("audit.jsonl");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::fs::read_to_string(&audit_path).map_or(true, |audit| {
        !audit.contains("\"action\":\"gateway.started\"")
    }) && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(50));
    }
    let audit = std::fs::read_to_string(&audit_path).unwrap_or_default();
    let mut stderr = String::new();
    if !audit.contains("\"action\":\"gateway.started\"") {
        let _ = child.kill();
        let _ = child.wait();
        if let Some(mut output) = child.stderr.take() {
            output.read_to_string(&mut stderr).unwrap();
        }
    }
    assert!(
        audit.contains("\"action\":\"gateway.started\""),
        "expected gateway.started in {}, found {audit:?}; child stderr: {stderr}",
        audit_path.display()
    );
    assert!(
        std::fs::read_dir(&manifest_dir).is_ok_and(|mut entries| entries.next().is_some()),
        "expected {} to contain a SlateDB manifest",
        manifest_dir.display()
    );

    let gateway_shard_dir = storage.join("gateway-shard");
    assert!(
        !gateway_shard_dir.exists(),
        "`--role all` must not create a second, unreferenced gateway-shard/ directory, found {}",
        gateway_shard_dir.display()
    );

    let shards_dir = storage.join("shards");
    let entries: Vec<_> = std::fs::read_dir(&shards_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(entries, [std::ffi::OsString::from("0")]);

    // Trigger the same shutdown path an operator's SIGTERM would.
    let pid = child.id().to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("failed to invoke `kill`");
    assert!(status.success(), "kill -TERM failed to signal child");
    assert!(child.wait().unwrap().success());
}
