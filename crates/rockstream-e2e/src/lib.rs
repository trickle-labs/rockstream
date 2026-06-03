use std::path::PathBuf;
use std::process::Command;
use std::sync::Once;

static BUILD_ONCE: Once = Once::new();

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

/// Ensure the rockstream binary and the rockstream:test Docker image are built.
pub fn ensure_image_built() {
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
