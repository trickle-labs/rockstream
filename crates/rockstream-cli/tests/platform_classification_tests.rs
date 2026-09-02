//! Platform Matrix Classification & Startup Rejection Tests (v0.59.22 Slice 1 / Phase 3a).

use rockstream_types::platform::{ClassificationTier, PlatformClassifier};

#[test]
fn test_platform_matrix_classification_and_startup_rejection() {
    // 1. Architecture Evaluation
    let x86_eval = PlatformClassifier::evaluate_arch("x86_64");
    assert_eq!(x86_eval.tier, ClassificationTier::Supported);
    assert_eq!(x86_eval.name, "x86_64");

    let arm_eval = PlatformClassifier::evaluate_arch("aarch64");
    assert_eq!(arm_eval.tier, ClassificationTier::Supported);
    assert_eq!(arm_eval.name, "aarch64");

    let riscv_eval = PlatformClassifier::evaluate_arch("riscv64");
    assert_eq!(riscv_eval.tier, ClassificationTier::CompatibleUnverified);

    let ppc_eval = PlatformClassifier::evaluate_arch("ppc64le");
    assert_eq!(ppc_eval.tier, ClassificationTier::CompatibleUnverified);

    let i386_eval = PlatformClassifier::evaluate_arch("x86");
    assert_eq!(i386_eval.tier, ClassificationTier::Unsupported);
    assert!(i386_eval.reason.unwrap().contains("32-bit"));

    let arm32_eval = PlatformClassifier::evaluate_arch("arm");
    assert_eq!(arm32_eval.tier, ClassificationTier::Unsupported);

    // 2. OS Evaluation
    let linux_eval = PlatformClassifier::evaluate_os("linux");
    assert_eq!(linux_eval.tier, ClassificationTier::Supported);

    let macos_eval = PlatformClassifier::evaluate_os("macos");
    assert_eq!(macos_eval.tier, ClassificationTier::Supported);

    let wsl2_eval = PlatformClassifier::evaluate_os("wsl2");
    assert_eq!(wsl2_eval.tier, ClassificationTier::CompatibleUnverified);

    let win_eval = PlatformClassifier::evaluate_os("windows");
    assert_eq!(win_eval.tier, ClassificationTier::Unsupported);

    // 3. Libc Evaluation
    let libc_eval = PlatformClassifier::evaluate_libc();
    assert!(
        libc_eval.tier == ClassificationTier::Supported
            || libc_eval.tier == ClassificationTier::CompatibleUnverified
    );

    // 4. Host Environment Evaluation
    let host = PlatformClassifier::evaluate_host();
    assert!(host.is_allowed());

    // 5. Startup Validation on Current Host
    let startup_result = PlatformClassifier::validate_startup();
    assert!(startup_result.is_ok());

    // 6. External Storage Backend Evaluation
    let lfs_eval = PlatformClassifier::evaluate_storage_backend("/var/lib/rockstream");
    assert_eq!(lfs_eval.tier, ClassificationTier::Supported);

    let s3_eval = PlatformClassifier::evaluate_storage_backend("s3://prod-bucket/rockstream");
    assert_eq!(s3_eval.tier, ClassificationTier::Supported);

    let r2_eval =
        PlatformClassifier::evaluate_storage_backend("s3://bucket.r2.cloudflarestorage.com/data");
    assert_eq!(r2_eval.tier, ClassificationTier::CompatibleUnverified);

    let nfs_eval = PlatformClassifier::evaluate_storage_backend("nfs://nas.internal/mount/data");
    assert_eq!(nfs_eval.tier, ClassificationTier::Unsupported);

    // 7. Database Backend Evaluation
    let pg18_eval = PlatformClassifier::evaluate_postgres_version(18);
    assert_eq!(pg18_eval.tier, ClassificationTier::Supported);

    let pg16_eval = PlatformClassifier::evaluate_postgres_version(16);
    assert_eq!(pg16_eval.tier, ClassificationTier::Supported);

    let pg13_eval = PlatformClassifier::evaluate_postgres_version(13);
    assert_eq!(pg13_eval.tier, ClassificationTier::CompatibleUnverified);

    let pg11_eval = PlatformClassifier::evaluate_postgres_version(11);
    assert_eq!(pg11_eval.tier, ClassificationTier::Unsupported);

    // 8. Kafka Backend Evaluation
    let kafka3_eval = PlatformClassifier::evaluate_kafka_version("3.8.0");
    assert_eq!(kafka3_eval.tier, ClassificationTier::Supported);

    let redpanda_eval = PlatformClassifier::evaluate_kafka_version("redpanda-24.1");
    assert_eq!(redpanda_eval.tier, ClassificationTier::Supported);

    let kafka28_eval = PlatformClassifier::evaluate_kafka_version("2.8.1");
    assert_eq!(kafka28_eval.tier, ClassificationTier::CompatibleUnverified);

    let kafka_old_eval = PlatformClassifier::evaluate_kafka_version("2.4.0");
    assert_eq!(kafka_old_eval.tier, ClassificationTier::Unsupported);
}
