//! Explicit loopback development keeps the control daemon usable without
//! opening an unauthenticated public management listener.

use rockstream_types::ids::WorkloadId;
use rockstream_types::topology::{ControlMessage, WorkerMessage};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct RunningControl(Child);

impl Drop for RunningControl {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn loopback_dev_daemon_serves_control_without_a_public_management_listener() {
    let temp = tempfile::tempdir().unwrap();
    let control_addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let management_addr = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let log_path = temp.path().join("control.stderr");
    let mut process = RunningControl(
        Command::new(env!("CARGO_BIN_EXE_rockstream"))
            .current_dir(temp.path())
            .args([
                "start",
                "--role",
                "control",
                "--control-bind",
                &control_addr.to_string(),
                "--management-addr",
                &management_addr.to_string(),
                "--storage",
                temp.path().join("storage").to_str().unwrap(),
                "--allow-insecure-internal-dev",
                "--daemon",
            ])
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log_path).unwrap())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut stream = loop {
        if let Ok(stream) = TcpStream::connect(control_addr) {
            break stream;
        }
        let status = process.0.try_wait().unwrap();
        assert!(
            status.is_none() && Instant::now() < deadline,
            "control did not start: {status:?}; {}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = WorkerMessage::GetWorkloadTopology {
        workload_id: WorkloadId(17),
    };
    writeln!(stream, "{}", serde_json::to_string(&request).unwrap()).unwrap();
    let mut response = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut response)
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response).unwrap(),
        serde_json::to_value(ControlMessage::OperationFailed {
            code: "RS-0001".to_owned(),
            message: "workload is not deployed".to_owned(),
            next_steps: "Retry after the workload and workers are ready.".to_owned(),
        })
        .unwrap()
    );
    let management_port = TcpListener::bind(management_addr)
        .expect("plaintext development must leave the public management port closed");
    assert_eq!(management_port.local_addr().unwrap(), management_addr);
    assert_eq!(process.0.try_wait().unwrap(), None);
}
