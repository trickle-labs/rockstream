pub mod external_harness;
pub mod pki;

pub fn docker_available() -> bool {
    docker_available_when(
        std::process::Command::new("docker")
            .args(["info"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success()),
        std::env::var_os("ROCKSTREAM_REQUIRE_DOCKER").is_some(),
    )
}

fn docker_available_when(available: bool, required: bool) -> bool {
    assert!(
        available || !required,
        "ROCKSTREAM_REQUIRE_DOCKER=1 requires Docker; start Docker or remove the CI-only requirement"
    );
    available
}

#[cfg(test)]
mod tests {
    use super::docker_available_when;

    #[test]
    fn unavailable_docker_skips_locally() {
        assert!(!docker_available_when(false, false));
    }

    #[test]
    #[should_panic(expected = "ROCKSTREAM_REQUIRE_DOCKER=1 requires Docker")]
    fn unavailable_docker_fails_when_required() {
        docker_available_when(false, true);
    }
}
