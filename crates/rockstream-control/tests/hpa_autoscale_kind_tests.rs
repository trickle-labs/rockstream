use std::fs;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn command_available(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn docker_available() -> bool {
    command_available("docker", &["info"])
}

fn kind_available() -> bool {
    command_available("kind", &["version"])
}

fn kubectl_available() -> bool {
    command_available("kubectl", &["version", "--client"])
}

fn rockstream_ci_image_available() -> bool {
    command_available("docker", &["image", "inspect", "rockstream:ci"])
}

fn skip_message_for(
    docker_ok: bool,
    kind_ok: bool,
    kubectl_ok: bool,
    image_ok: bool,
) -> Option<String> {
    let mut missing = Vec::new();
    if !docker_ok {
        missing.push("docker");
    }
    if !kind_ok {
        missing.push("kind");
    }
    if !kubectl_ok {
        missing.push("kubectl");
    }
    if !image_ok {
        missing.push("rockstream:ci image");
    }
    if missing.is_empty() {
        None
    } else {
        Some(format!(
            "SKIP real_hpa_scales_out_and_in: required tooling unavailable ({})",
            missing.join(", ")
        ))
    }
}

fn hpa_skip_reason() -> Option<String> {
    skip_message_for(
        docker_available(),
        kind_available(),
        kubectl_available(),
        rockstream_ci_image_available(),
    )
}

fn run_checked(program: &str, args: &[&str]) {
    let status = Command::new(program).args(args).status().unwrap();
    assert!(
        status.success(),
        "command failed: {} {}",
        program,
        args.join(" ")
    );
}

fn run_capture(program: &str, args: &[&str]) -> String {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "command failed: {} {}",
        program,
        args.join(" ")
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

struct KindClusterGuard {
    name: String,
}

impl Drop for KindClusterGuard {
    fn drop(&mut self) {
        let _ = Command::new("kind")
            .args(["delete", "cluster", "--name", &self.name])
            .status();
    }
}

fn write_manifests(dir: &std::path::Path) {
    fs::write(
        dir.join("deployment.yaml"),
        "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: rockstream-hpa\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: rockstream-hpa\n  template:\n    metadata:\n      labels:\n        app: rockstream-hpa\n    spec:\n      containers:\n      - name: rockstream\n        image: rockstream:ci\n        args: [\"start\", \"--storage=/data\", \"--role=all\", \"--metrics-bind=0.0.0.0:9100\"]\n        ports:\n        - containerPort: 9100\n",
    )
    .unwrap();
    fs::write(
        dir.join("service.yaml"),
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: rockstream-hpa\nspec:\n  selector:\n    app: rockstream-hpa\n  ports:\n  - name: metrics\n    port: 9100\n    targetPort: 9100\n",
    )
    .unwrap();
    fs::write(
        dir.join("hpa.yaml"),
        "apiVersion: autoscaling/v2\nkind: HorizontalPodAutoscaler\nmetadata:\n  name: rockstream-hpa\nspec:\n  scaleTargetRef:\n    apiVersion: apps/v1\n    kind: Deployment\n    name: rockstream-hpa\n  minReplicas: 1\n  maxReplicas: 4\n  metrics:\n  - type: Pods\n    pods:\n      metric:\n        name: cluster_worker_pressure\n      target:\n        type: AverageValue\n        averageValue: \"1\"\n",
    )
    .unwrap();
}

fn wait_for_pod_floor(name: &str, expected_at_least: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let pods = run_capture(
            "kubectl",
            &["get", "pods", "-l", &format!("app={name}"), "-o", "name"],
        );
        let count = pods.lines().filter(|line| !line.trim().is_empty()).count();
        if count >= expected_at_least {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {name} to reach {expected_at_least} pods"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn wait_for_pod_ceiling(name: &str, expected_at_most: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let pods = run_capture(
            "kubectl",
            &["get", "pods", "-l", &format!("app={name}"), "-o", "name"],
        );
        let count = pods.lines().filter(|line| !line.trim().is_empty()).count();
        if count <= expected_at_most {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {name} to fall to <= {expected_at_most} pods"
        );
        std::thread::sleep(Duration::from_secs(5));
    }
}

#[test]
fn kind_skip_message_is_clear() {
    let message = skip_message_for(false, false, false, false).unwrap();
    assert!(message.contains("docker"));
    assert!(message.contains("kind"));
    assert!(message.contains("kubectl"));
    assert!(message.contains("rockstream:ci image"));
}

#[test]
fn real_hpa_scales_out_and_in() {
    if let Some(reason) = hpa_skip_reason() {
        eprintln!("{reason}");
        return;
    }

    let cluster_name = format!("rockstream-v047-{}", std::process::id());
    let dir = tempfile::tempdir().unwrap();
    write_manifests(dir.path());

    run_checked("kind", &["create", "cluster", "--name", &cluster_name]);
    run_checked(
        "kind",
        &[
            "load",
            "docker-image",
            "rockstream:ci",
            "--name",
            &cluster_name,
        ],
    );

    let cleanup = KindClusterGuard {
        name: cluster_name.clone(),
    };

    run_checked(
        "kubectl",
        &[
            "apply",
            "-f",
            dir.path().join("deployment.yaml").to_str().unwrap(),
        ],
    );
    run_checked(
        "kubectl",
        &[
            "apply",
            "-f",
            dir.path().join("service.yaml").to_str().unwrap(),
        ],
    );
    run_checked(
        "kubectl",
        &["apply", "-f", dir.path().join("hpa.yaml").to_str().unwrap()],
    );

    wait_for_pod_floor("rockstream-hpa", 1, Duration::from_secs(60));

    run_checked(
        "kubectl",
        &[
            "set",
            "env",
            "deployment/rockstream-hpa",
            "ROCKSTREAM_TEST_DEMANDED_SHARDS=10",
            "ROCKSTREAM_TEST_PLACED_SHARDS=1",
        ],
    );
    wait_for_pod_floor("rockstream-hpa", 2, Duration::from_secs(120));

    run_checked(
        "kubectl",
        &[
            "set",
            "env",
            "deployment/rockstream-hpa",
            "ROCKSTREAM_TEST_DEMANDED_SHARDS=1",
            "ROCKSTREAM_TEST_PLACED_SHARDS=1",
        ],
    );
    wait_for_pod_ceiling("rockstream-hpa", 1, Duration::from_secs(600));

    drop(cleanup);
}
