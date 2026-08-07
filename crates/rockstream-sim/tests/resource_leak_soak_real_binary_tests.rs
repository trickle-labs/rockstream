//! Real unified-binary resource soak for v0.51.20.
//!
//! This target is deliberately Docker-only. Its scheduled invocation supplies
//! the four-hour wall-clock budget; developers may set a short duration while
//! retaining the identical pgwire/source/view workload and resource gate.

#![cfg(feature = "docker_tests")]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

use hmac::{Hmac, Mac};
use rockstream_sim::{ProcessResourceSampler, ResourceGateConfig, ResourceSeriesGate};
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::sleep,
};
use tokio_postgres::NoTls;

const IMAGE_NAME: &str = "rockstream-tc-test";
const IMAGE_TAG: &str = "latest";
const CONNECTIONS_PER_CYCLE: usize = 64;
const INJECTED_CONNECTIONS_PER_CYCLE: usize = 72;
const INJECTED_RETAINED_CONNECTION_LIMIT: usize = 216;
const MINIO_BUCKET: &str = "rockstream-resource-soak";
const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .map(|value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
        })
        .unwrap_or(default)
}

fn docker(args: &[&str]) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .unwrap_or_else(|error| panic!("Docker is required for resource soak: {error}"));
    assert!(status.success(), "docker {} failed", args.join(" "));
}

fn docker_output(args: &[&str]) -> String {
    let output = Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("docker {} could not start: {error}", args.join(" ")));
    assert!(
        output.status.success(),
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn docker_owned(args: &[String]) {
    let status = Command::new("docker")
        .args(args)
        .status()
        .unwrap_or_else(|error| panic!("Docker is required for resource soak: {error}"));
    assert!(status.success(), "docker {} failed", args.join(" "));
}

fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key is valid");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86_400;
    let mut days = (secs / 86_400) as u32;
    let hour = (sod / 3_600) as u32;
    let minute = ((sod % 3_600) / 60) as u32;
    let second = (sod % 60) as u32;
    let mut year = 1970_u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let days_in_year = if leap { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days_in_month = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0_u32;
    for days_in_current_month in days_in_month {
        if days < days_in_current_month {
            break;
        }
        days -= days_in_current_month;
        month += 1;
    }
    (year, month + 1, days + 1, hour, minute, second)
}

async fn create_minio_bucket(port: u16) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_secs();
    let (year, month, day, hour, minute, second) = epoch_to_ymd_hms(secs);
    let date = format!("{year:04}{month:02}{day:02}");
    let datetime = format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z");
    let host = format!("127.0.0.1:{port}");
    let empty_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{MINIO_BUCKET}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let first = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let second = hmac_sha256(&first, b"us-east-1");
    let third = hmac_sha256(&second, b"s3");
    let signing_key = hmac_sha256(&third, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let response = reqwest::Client::new()
        .put(format!("http://{host}/{MINIO_BUCKET}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", &authorization)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("MinIO bucket creation request must start");
    assert!(
        response.status().is_success() || response.status().as_u16() == 409,
        "MinIO bucket creation failed: {}",
        response.status()
    );
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO>, u16) {
    let container = MinIO::default()
        .start()
        .await
        .expect("MinIO must start for the real-binary resource soak");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("MinIO API port must be mapped");
    create_minio_bucket(port).await;
    (container, port)
}

fn sample_container_resources(name: &str, timestamp_secs: u64) -> rockstream_sim::ResourceSample {
    let output = docker_output([
        "exec",
        name,
        "sh",
        "-c",
        "awk '/^VmRSS:/ {print $2; exit}' /proc/1/status; ls -1 /proc/1/fd 2>/dev/null | wc -l; for fd in /proc/1/fd/*; do readlink \"$fd\"; done 2>/dev/null | grep -c '^socket:\\[' || true",
    ]
    .as_slice());
    let values = output
        .split_whitespace()
        .map(|value| {
            value
                .parse::<u64>()
                .expect("container resource sample must be numeric")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values.len(),
        3,
        "container resource sample must contain RSS, FD, socket"
    );
    rockstream_sim::ResourceSample {
        timestamp_secs,
        rss_kib: values[0],
        open_fds: values[1],
        open_sockets: values[2],
    }
}

struct ContainerCleanup(String);

impl Drop for ContainerCleanup {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.0])
            .status();
    }
}

async fn read_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut kind = [0_u8; 1];
    stream.read_exact(&mut kind).await.unwrap();
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await.unwrap();
    let body_length = u32::from_be_bytes(length) as usize - 4;
    let mut body = vec![0_u8; body_length];
    stream.read_exact(&mut body).await.unwrap();
    (kind[0], body)
}

async fn startup(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut body = Vec::from(196_608_u32.to_be_bytes());
    body.extend_from_slice(b"user\0soak\0database\0soak\0\0");
    let mut message = Vec::from(((body.len() + 4) as u32).to_be_bytes());
    message.extend_from_slice(&body);
    stream.write_all(&message).await.unwrap();
    loop {
        let (kind, _) = read_frame(&mut stream).await;
        if kind == b'Z' {
            return stream;
        }
    }
}

async fn command_tag(port: u16, sql: &str) -> String {
    let mut stream = startup(port).await;
    command_tag_on_stream(&mut stream, sql).await
}

async fn command_tag_on_stream(stream: &mut TcpStream, sql: &str) -> String {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    let mut message = vec![b'Q'];
    message.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    message.extend_from_slice(&body);
    stream.write_all(&message).await.unwrap();
    let mut tag = None;
    loop {
        let (kind, body) = read_frame(stream).await;
        if kind == b'C' {
            tag = Some(String::from_utf8(body[..body.len() - 1].to_vec()).unwrap());
        }
        if kind == b'E' {
            panic!(
                "{sql} returned pgwire error: {}",
                String::from_utf8_lossy(&body)
            );
        }
        if kind == b'Z' {
            break;
        }
    }
    tag.unwrap_or_else(|| panic!("{sql} did not return a CommandComplete tag"))
}

async fn command_tags(port: u16, sqls: &[String]) -> Vec<String> {
    let mut stream = startup(port).await;
    let mut tags = Vec::with_capacity(sqls.len());
    for sql in sqls {
        tags.push(command_tag_on_stream(&mut stream, sql).await);
    }
    tags
}

async fn active_view_ids(port: u16) -> Vec<i64> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=soak dbname=soak"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query("SELECT id FROM soak_view ORDER BY id", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            row.get::<_, String>(0)
                .parse::<i64>()
                .expect("materialized view id must be an integer")
        })
        .collect()
}

async fn wait_for_gateway(port: u16) {
    for _ in 0..60 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            // A bound listener can precede gateway task initialization. Keep
            // the readiness probe itself connection-only so it exercises the
            // abnormal EOF cleanup path, then allow the protocol task to
            // complete its startup before sending the first StartupMessage.
            sleep(Duration::from_secs(1)).await;
            return;
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("unified rockstream binary did not open its pgwire listener");
}

fn resource_soak_artifact_dir() -> Option<PathBuf> {
    let directory = env::var("ROCKSTREAM_RESOURCE_SOAK_ARTIFACT_DIR").ok()?;
    let path = PathBuf::from(directory);
    Some(if path.is_absolute() {
        path
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    })
}

fn write_samples_if_requested(samples: &[rockstream_sim::ResourceSample]) {
    let Some(directory) = resource_soak_artifact_dir() else {
        return;
    };
    fs::create_dir_all(&directory).unwrap();
    let tsv = samples
        .iter()
        .map(|sample| {
            format!(
                "{}\t{}\t{}\t{}",
                sample.timestamp_secs, sample.rss_kib, sample.open_fds, sample.open_sockets
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(
        Path::new(&directory).join("resource-leak-soak-samples.tsv"),
        format!("{tsv}\n"),
    )
    .unwrap();
}

fn start_workflow_sampler(pid: u32, duration_secs: u64, interval_secs: u64) -> Option<Child> {
    let artifact_dir = resource_soak_artifact_dir()?;
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    Some(
        Command::new("bash")
            .current_dir(workspace)
            .arg("scripts/sample-resource-leak-soak.sh")
            .args([
                "--pid",
                &pid.to_string(),
                "--artifact-dir",
                artifact_dir.to_str().expect("artifact path must be UTF-8"),
                "--duration-secs",
                &duration_secs.to_string(),
                "--interval-secs",
                &interval_secs.to_string(),
            ])
            .spawn()
            .expect("workflow resource sampler must start"),
    )
}

async fn run_soak(inject_teardown_leak: bool, use_minio: bool) {
    docker(&["info"]);
    docker(&["image", "inspect", &format!("{IMAGE_NAME}:{IMAGE_TAG}")]);
    let minio = if use_minio {
        Some(start_minio().await)
    } else {
        None
    };
    let name = format!("rs-v05120-resource-soak-{}", std::process::id());
    let mut run_args = vec![
        "run".to_owned(),
        "--detach".to_owned(),
        "--rm".to_owned(),
        "--name".to_owned(),
        name.clone(),
        "--publish".to_owned(),
        "127.0.0.1::5432".to_owned(),
    ];
    if let Some((_, minio_port)) = &minio {
        run_args.extend([
            "--add-host".to_owned(),
            "host.docker.internal:host-gateway".to_owned(),
            "--env".to_owned(),
            "ROCKSTREAM_OBJECT_STORE_ENDPOINT=http://host.docker.internal:".to_owned()
                + &minio_port.to_string(),
            "--env".to_owned(),
            format!("ROCKSTREAM_OBJECT_STORE_BUCKET={MINIO_BUCKET}"),
            "--env".to_owned(),
            "ROCKSTREAM_OBJECT_STORE_REGION=us-east-1".to_owned(),
            "--env".to_owned(),
            format!("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY={MINIO_USER}"),
            "--env".to_owned(),
            format!("ROCKSTREAM_OBJECT_STORE_SECRET_KEY={MINIO_PASS}"),
        ]);
    }
    run_args.extend([
        IMAGE_NAME.to_owned(),
        "start".to_owned(),
        "--storage=/data".to_owned(),
        "--role=all".to_owned(),
        "--listen=0.0.0.0:5432".to_owned(),
    ]);
    docker_owned(&run_args);
    let _cleanup = ContainerCleanup(name.clone());
    let port = docker_output(&["port", &name, "5432/tcp"])
        .trim()
        .rsplit(':')
        .next()
        .unwrap()
        .parse::<u16>()
        .unwrap();
    wait_for_gateway(port).await;

    assert_eq!(
        command_tag(port, "CREATE TABLE soak_rows (id BIGINT)").await,
        "CREATE TABLE 0"
    );
    assert_eq!(
        command_tag(
            port,
            "CREATE MATERIALIZED VIEW soak_view AS SELECT id FROM soak_rows"
        )
        .await,
        "CREATE MATERIALIZED VIEW 0"
    );
    assert_eq!(
        command_tag(port, "CREATE SOURCE soak_source TYPE kafka (bootstrap.servers='127.0.0.1:9092', topic='resource-soak') FORMAT json").await,
        "CREATE SOURCE 0"
    );

    let duration_secs = env_u64("ROCKSTREAM_RESOURCE_SOAK_DURATION_SECS", 14_400);
    let interval_secs = env_u64("ROCKSTREAM_RESOURCE_SOAK_SAMPLE_INTERVAL_SECS", 60);
    assert!(interval_secs > 0, "resource-soak interval must be nonzero");
    let pid = docker_output(&["inspect", "--format", "{{.State.Pid}}", &name])
        .trim()
        .parse::<u32>()
        .unwrap();
    let host_pid_is_visible = Path::new(&format!("/proc/{pid}/status")).is_file();
    let mut sampler = host_pid_is_visible
        .then(|| ProcessResourceSampler::new(pid, duration_secs, interval_secs).unwrap());
    let capacity = ResourceGateConfig::max_samples(duration_secs, interval_secs);
    let mut container_samples = Vec::with_capacity(capacity);
    let mut workflow_sampler = if inject_teardown_leak {
        None
    } else if host_pid_is_visible {
        start_workflow_sampler(pid, duration_secs, interval_secs)
    } else {
        None
    };
    let mut retained_connections = Vec::new();
    let mut elapsed_secs = 0;
    let mut completed_cycles = 0_i64;
    loop {
        if let Some(sampler) = sampler.as_mut() {
            sampler.sample(elapsed_secs).unwrap();
        } else {
            container_samples.push(sample_container_resources(&name, elapsed_secs));
        }
        completed_cycles += 1;
        let insert_tags = command_tags(
            port,
            &[
                format!("INSERT INTO soak_rows VALUES ({completed_cycles})"),
                "COMMIT".to_owned(),
            ],
        )
        .await;
        assert_eq!(insert_tags, ["INSERT 0 1", "COMMIT"]);
        assert_eq!(
            command_tag(port, "ALTER SOURCE soak_source PAUSE").await,
            "ALTER SOURCE 0"
        );
        assert_eq!(
            command_tag(port, "ALTER SOURCE soak_source RESUME").await,
            "ALTER SOURCE 0"
        );
        assert_eq!(
            command_tag(port, "REFRESH MATERIALIZED VIEW soak_view").await,
            "REFRESH MATERIALIZED VIEW 0"
        );
        assert_eq!(
            active_view_ids(port).await,
            (1..=completed_cycles).collect::<Vec<_>>(),
            "materialized view must expose the complete incrementally maintained result"
        );
        let connections_this_cycle = if inject_teardown_leak {
            INJECTED_CONNECTIONS_PER_CYCLE
                .min(INJECTED_RETAINED_CONNECTION_LIMIT.saturating_sub(retained_connections.len()))
        } else {
            CONNECTIONS_PER_CYCLE
        };
        for _ in 0..connections_this_cycle {
            let stream = startup(port).await;
            if inject_teardown_leak
                && retained_connections.len() < INJECTED_RETAINED_CONNECTION_LIMIT
            {
                retained_connections.push(stream);
            } else {
                drop(stream);
            }
        }
        if elapsed_secs >= duration_secs {
            break;
        }
        sleep(Duration::from_secs(interval_secs)).await;
        elapsed_secs = elapsed_secs.saturating_add(interval_secs);
    }
    let config = ResourceGateConfig {
        capacity,
        warmup_samples: 2,
        rolling_window: 2,
        rss_tolerance_kib: 262_144,
        open_fd_tolerance: 128,
        open_socket_tolerance: 128,
    };
    if let Some(sampler_process) = workflow_sampler.as_mut() {
        assert!(
            sampler_process.wait().unwrap().success(),
            "workflow sampler must accept the real binary's bounded resource series"
        );
    } else {
        let samples = sampler
            .as_ref()
            .map(ProcessResourceSampler::samples)
            .unwrap_or(&container_samples);
        write_samples_if_requested(samples);
    }
    let samples = sampler
        .as_ref()
        .map(ProcessResourceSampler::samples)
        .unwrap_or(&container_samples);
    let result = ResourceSeriesGate::new(config).evaluate(samples);
    if inject_teardown_leak {
        assert!(
            result.is_err(),
            "test-only deregistration leak must fail the identical resource trend gate"
        );
    } else {
        let summary = result
            .expect("real binary resource series must remain within the flat-use tolerance band");
        assert!(summary.passed());
    }
    drop(retained_connections);
}

#[tokio::test]
async fn resource_leak_soak_real_binary_lfs_churn_is_flat() {
    run_soak(false, false).await;
}

#[tokio::test]
async fn resource_leak_soak_real_binary_minio_churn_is_flat() {
    run_soak(false, true).await;
}

#[tokio::test]
async fn resource_leak_soak_real_binary_injected_teardown_deregistration_leak_fails_gate() {
    run_soak(true, false).await;
}
