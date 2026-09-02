//! Kubernetes Manifests & Minimal Helm Chart Conformance Tests (v0.59.22 Slice 4 / Phase 3a).

use std::fs;
use std::path::Path;

#[test]
fn test_kubernetes_manifests_and_helm_chart_conformance() {
    let k8s_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/kubernetes");

    let gateway_dep = fs::read_to_string(k8s_dir.join("gateway-deployment.yaml"))
        .expect("gateway-deployment.yaml must exist");
    let worker_sts = fs::read_to_string(k8s_dir.join("worker-statefulset.yaml"))
        .expect("worker-statefulset.yaml must exist");
    let control_sts = fs::read_to_string(k8s_dir.join("control-statefulset.yaml"))
        .expect("control-statefulset.yaml must exist");

    for (name, manifest) in [
        ("gateway", &gateway_dep),
        ("worker", &worker_sts),
        ("control", &control_sts),
    ] {
        // 1. Pod Security Standards (Restricted)
        assert!(
            manifest.contains("runAsNonRoot: true"),
            "{name} must set runAsNonRoot: true"
        );
        assert!(
            manifest.contains("runAsUser: 10001"),
            "{name} must set runAsUser: 10001"
        );
        assert!(
            manifest.contains("runAsGroup: 10001"),
            "{name} must set runAsGroup: 10001"
        );
        assert!(
            manifest.contains("readOnlyRootFilesystem: true"),
            "{name} must set readOnlyRootFilesystem: true"
        );
        assert!(
            manifest.contains("allowPrivilegeEscalation: false"),
            "{name} must set allowPrivilegeEscalation: false"
        );
        assert!(
            manifest.contains("drop:\n                - ALL")
                || manifest.contains("drop: [\"ALL\"]")
                || manifest.contains("drop: [ALL]"),
            "{name} must drop all capabilities"
        );

        // 2. Health & Lifecycle Probes
        assert!(
            manifest.contains("startupProbe:"),
            "{name} must configure startupProbe"
        );
        assert!(
            manifest.contains("livenessProbe:"),
            "{name} must configure livenessProbe"
        );
        assert!(
            manifest.contains("readinessProbe:"),
            "{name} must configure readinessProbe"
        );
        assert!(manifest.contains("path: /live"), "{name} must query /live");
        assert!(
            manifest.contains("path: /ready"),
            "{name} must query /ready"
        );
    }

    // 3. Helm Chart Files
    let helm_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/helm/rockstream");

    let chart_yaml =
        fs::read_to_string(helm_dir.join("Chart.yaml")).expect("Chart.yaml must exist");
    assert!(chart_yaml.contains("name: rockstream"));
    assert!(chart_yaml.contains("version: 0.59.22"));

    let values_yaml =
        fs::read_to_string(helm_dir.join("values.yaml")).expect("values.yaml must exist");
    assert!(values_yaml.contains("runAsUser: 10001"));
    assert!(values_yaml.contains("readOnlyRootFilesystem: true"));
}
