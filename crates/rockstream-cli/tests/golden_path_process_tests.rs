//! Golden path process integration test executing the complete CLI workflow (Slice 6).

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

struct ProcessGuard(Option<Child>);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    let addr = format!("127.0.0.1:{port}");
    while start.elapsed() < timeout {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[test]
fn full_golden_path_workflow_end_to_end() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let temp_workspace = TempDir::new().expect("temp workspace");
    let workspace_dir = temp_workspace.path();

    // 1. rockstream project new sales
    let new_out = Command::new(binary)
        .current_dir(workspace_dir)
        .args(["project", "new", "sales"])
        .output()
        .expect("rockstream project new");

    assert!(
        new_out.status.success(),
        "project new must succeed: {}",
        String::from_utf8_lossy(&new_out.stderr)
    );
    let sales_dir = workspace_dir.join("sales");
    assert!(sales_dir.join("rockstream.toml").exists());
    assert!(sales_dir.join("project.toml").exists());
    assert!(sales_dir.join("schema.sql").exists());
    assert!(sales_dir.join("data/seed.csv").exists());
    assert!(sales_dir.join("queries/verify.sql").exists());
    assert!(sales_dir.join("README.md").exists());

    // 2. Start temporary RockStream node
    let port = free_port();
    let endpoint = format!("127.0.0.1:{port}");
    let storage_dir = sales_dir.join("storage");

    let node_child = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.to_str().unwrap(),
            "--listen",
            &endpoint,
            "--daemon",
        ])
        .spawn()
        .expect("start gateway node");
    let _guard = ProcessGuard(Some(node_child));

    assert!(
        wait_for_port(port, Duration::from_secs(10)),
        "gateway must listen on port {port}"
    );

    // 3. rockstream project apply
    let apply_out = Command::new(binary)
        .current_dir(&sales_dir)
        .args(["project", "apply", "--endpoint", &endpoint])
        .output()
        .expect("rockstream project apply");

    assert!(
        apply_out.status.success(),
        "project apply must succeed: {}",
        String::from_utf8_lossy(&apply_out.stderr)
    );
    let apply_stdout = String::from_utf8_lossy(&apply_out.stdout);
    assert!(apply_stdout.contains("Project 'sales' applied successfully"));

    // 4. rockstream project verify
    let verify_out = Command::new(binary)
        .current_dir(&sales_dir)
        .args(["project", "verify", "--endpoint", &endpoint])
        .output()
        .expect("rockstream project verify");

    assert!(
        verify_out.status.success(),
        "project verify must succeed: {}",
        String::from_utf8_lossy(&verify_out.stderr)
    );
    let verify_stdout = String::from_utf8_lossy(&verify_out.stdout);
    assert!(verify_stdout.contains("PASSED verification 'sales_by_store' (2 rows)"));

    // 5. DML Mutations via rockstream query and verify exact materialized view updates
    // A. INSERT
    let insert_out = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "query",
            "--endpoint",
            &endpoint,
            "INSERT INTO orders (id, store_id, amount) VALUES (4, 100, 30);",
        ])
        .output()
        .expect("insert query");
    assert!(
        insert_out.status.success(),
        "insert must succeed: {}",
        String::from_utf8_lossy(&insert_out.stderr)
    );

    let select_out1 = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "query",
            "--endpoint",
            &endpoint,
            "--format",
            "csv",
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
        ])
        .output()
        .expect("select query");
    assert!(select_out1.status.success());
    let csv1 = String::from_utf8_lossy(&select_out1.stdout);
    let lines1: Vec<&str> = csv1.trim().lines().collect();
    assert_eq!(lines1, vec!["store_id,total_amount", "100,150", "200,40"]);

    // B. UPDATE
    let update_out = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "query",
            "--endpoint",
            &endpoint,
            "UPDATE orders SET amount = 80 WHERE id = 1, store_id = 100, amount = 50;",
        ])
        .output()
        .expect("update query");
    assert!(
        update_out.status.success(),
        "update must succeed: {}",
        String::from_utf8_lossy(&update_out.stderr)
    );

    let select_out2 = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "query",
            "--endpoint",
            &endpoint,
            "--format",
            "csv",
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
        ])
        .output()
        .expect("select query");
    assert!(select_out2.status.success());
    let csv2 = String::from_utf8_lossy(&select_out2.stdout);
    let lines2: Vec<&str> = csv2.trim().lines().collect();
    assert_eq!(lines2, vec!["store_id,total_amount", "100,180", "200,40"]);

    // C. DELETE
    let delete_out = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "query",
            "--endpoint",
            &endpoint,
            "DELETE FROM orders WHERE id = 3, store_id = 200, amount = 40;",
        ])
        .output()
        .expect("delete query");
    assert!(
        delete_out.status.success(),
        "delete must succeed: {}",
        String::from_utf8_lossy(&delete_out.stderr)
    );

    let select_out3 = Command::new(binary)
        .current_dir(&sales_dir)
        .args([
            "query",
            "--endpoint",
            &endpoint,
            "--format",
            "csv",
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
        ])
        .output()
        .expect("select query");
    assert!(select_out3.status.success());
    let csv3 = String::from_utf8_lossy(&select_out3.stdout);
    let lines3: Vec<&str> = csv3.trim().lines().collect();
    assert_eq!(lines3, vec!["store_id,total_amount", "100,180"]);
}
