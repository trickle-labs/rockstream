//! v0.62 Slice 4 — Explicit Storage URL Parsing, Scheme Validation & Backend Binding Tests

use std::path::{Path, PathBuf};
use tempfile::tempdir;

use rockstream_types::config::StorageUrl;

#[test]
fn test_storage_url_parsing_and_scheme_validation() {
    // 1. Valid absolute file URL
    let u1 = StorageUrl::parse("file:///var/lib/rockstream/data").expect("parse absolute file URL");
    assert_eq!(u1.scheme(), "file");
    assert!(u1.is_file());
    assert!(!u1.is_s3());
    assert_eq!(
        u1.as_file_path(),
        Some(Path::new("/var/lib/rockstream/data"))
    );
    assert_eq!(u1.to_string(), "file:///var/lib/rockstream/data");

    // 2. Valid relative file URL
    let u2 = StorageUrl::parse("file://./data").expect("parse relative file URL");
    assert_eq!(u2.scheme(), "file");
    assert!(u2.is_file());
    assert_eq!(u2.as_file_path(), Some(Path::new("./data")));
    assert_eq!(u2.to_string(), "file://./data");

    // 3. Valid S3 URL with bucket and prefix
    let u3 =
        StorageUrl::parse("s3://rockstream-bucket/shards/prod").expect("parse s3 URL with prefix");
    assert_eq!(u3.scheme(), "s3");
    assert!(u3.is_s3());
    assert!(!u3.is_file());
    assert_eq!(u3.s3_bucket(), Some("rockstream-bucket"));
    assert_eq!(u3.s3_prefix(), Some("shards/prod"));
    assert_eq!(u3.to_string(), "s3://rockstream-bucket/shards/prod");

    // 4. Valid S3 URL with bucket only
    let u4 = StorageUrl::parse("s3://rockstream-bucket").expect("parse s3 URL bucket only");
    assert_eq!(u4.s3_bucket(), Some("rockstream-bucket"));
    assert_eq!(u4.s3_prefix(), Some(""));
    assert_eq!(u4.to_string(), "s3://rockstream-bucket");

    // 5. Raw path without scheme defaults to file
    let u5 = StorageUrl::parse("/tmp/rockstream-store").expect("parse raw path");
    assert_eq!(u5, StorageUrl::File(PathBuf::from("/tmp/rockstream-store")));

    // 6. S3 without bucket must fail with RS-0002
    let err_empty_bucket = StorageUrl::parse("s3://").unwrap_err();
    assert!(
        err_empty_bucket.contains("RS-0002") && err_empty_bucket.contains("bucket"),
        "expected RS-0002 bucket error: {err_empty_bucket}"
    );

    // 7. Unsupported schemes must fail with RS-0002
    let err_http = StorageUrl::parse("http://example.com/storage").unwrap_err();
    assert!(
        err_http.contains("RS-0002") && err_http.contains("unsupported storage scheme `http`"),
        "expected RS-0002 for http: {err_http}"
    );

    let err_hdfs = StorageUrl::parse("hdfs://namenode:9000/data").unwrap_err();
    assert!(
        err_hdfs.contains("RS-0002") && err_hdfs.contains("unsupported storage scheme `hdfs`"),
        "expected RS-0002 for hdfs: {err_hdfs}"
    );

    let err_ftp = StorageUrl::parse("ftp://backup.example.com/rocks").unwrap_err();
    assert!(
        err_ftp.contains("RS-0002") && err_ftp.contains("unsupported storage scheme `ftp`"),
        "expected RS-0002 for ftp: {err_ftp}"
    );

    // 8. Round-trip Serde
    let serialized = serde_json::to_string(&u3).expect("serialize s3 url");
    let deserialized: StorageUrl = serde_json::from_str(&serialized).expect("deserialize s3 url");
    assert_eq!(u3, deserialized);
}

#[test]
fn test_relative_storage_url_resolves_against_config_dir() {
    let base_dir = Path::new("/etc/rockstream");

    // 1. Relative file URL resolves against base_dir
    let rel_url = StorageUrl::parse("file://./data").expect("parse relative url");
    let resolved = rel_url.resolve(base_dir);
    assert_eq!(resolved, StorageUrl::File(base_dir.join("./data")));
    assert_eq!(resolved.scheme(), "file");

    let sub_rel = StorageUrl::parse("file://shards/db").expect("parse sub relative url");
    let sub_resolved = sub_rel.resolve_relative(base_dir);
    assert_eq!(sub_resolved, StorageUrl::File(base_dir.join("shards/db")));

    // 2. Absolute file URL remains unchanged
    let abs_url = StorageUrl::parse("file:///var/data/rockstream").expect("parse abs url");
    let abs_resolved = abs_url.resolve(base_dir);
    assert_eq!(
        abs_resolved,
        StorageUrl::File(PathBuf::from("/var/data/rockstream"))
    );

    // 3. S3 URL remains unchanged when resolving relative to directory
    let s3_url = StorageUrl::parse("s3://bucket/prefix").expect("parse s3 url");
    let s3_resolved = s3_url.resolve(base_dir);
    assert_eq!(s3_resolved, s3_url);
}

#[test]
fn test_inaccessible_storage_fails_cleanly_without_fallback() {
    let tmp = tempdir().unwrap();
    let regular_file = tmp.path().join("a_file.txt");
    std::fs::write(&regular_file, b"cannot be a directory").unwrap();

    // Trying to create a subdirectory inside a regular file will fail with ENOTDIR on Unix
    let inaccessible_path = regular_file.join("sub_storage/shards");
    let inaccessible_url = StorageUrl::File(inaccessible_path);

    let err = inaccessible_url.verify_accessible().unwrap_err();
    assert!(
        err.contains("RS-0003"),
        "expected RS-0003 in accessibility error: {err}"
    );
    assert!(
        err.contains("inaccessible"),
        "expected 'inaccessible' in error: {err}"
    );

    // Accessible path succeeds
    let accessible_path = tmp.path().join("valid_storage/shards");
    let accessible_url = StorageUrl::File(accessible_path.clone());
    assert!(accessible_url.verify_accessible().is_ok());
    assert!(accessible_path.exists());
}
