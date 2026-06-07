use std::path::PathBuf;
use std::process::Command;
use std::sync::Once;

static BUILD_ONCE: Once = Once::new();
static DOCKER_AVAILABLE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static DOCKER_CHECK: Once = Once::new();

/// Find the workspace root by looking for Cargo.toml.
pub fn find_workspace_root() -> PathBuf {
    let mut dir = std::env::current_dir().expect("failed to get CWD");
    while !dir.join("Cargo.toml").exists() || !dir.join("crates").exists() {
        if let Some(parent) = dir.parent() {
            dir = parent.to_path_buf();
        } else {
            panic!(
                "Could not find workspace root starting from {:?}",
                std::env::current_dir()
            );
        }
    }
    dir
}

/// Check if Docker daemon is available
pub fn is_docker_available() -> bool {
    DOCKER_CHECK.call_once(|| {
        let available = Command::new("docker")
            .args(["ps"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        DOCKER_AVAILABLE.store(available, std::sync::atomic::Ordering::Relaxed);
    });
    DOCKER_AVAILABLE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Ensure the rockstream binary and the rockstream:test Docker image are built.
pub fn ensure_image_built() {
    if !is_docker_available() {
        eprintln!("Warning: Docker daemon not available, skipping E2E tests");
        return;
    }

    BUILD_ONCE.call_once(|| {
        let root = find_workspace_root();
        println!("Workspace root: {}", root.display());

        // Docker build the image (multi-stage compiles inside Docker)
        println!("Building rockstream:test Docker image...");
        let status = Command::new("docker")
            .current_dir(&root)
            .args(["build", "-t", "rockstream:test", "."])
            .status()
            .expect("failed to run docker build");
        assert!(status.success(), "docker build failed");
        println!("Docker image rockstream:test is ready.");
    });
}
