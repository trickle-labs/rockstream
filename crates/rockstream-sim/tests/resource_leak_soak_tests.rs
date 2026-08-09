use rockstream_sim::{
    ProcessResourceSampler, ResourceGateConfig, ResourceGateError, ResourceSample,
    ResourceSeriesGate,
};

const CONFIG: ResourceGateConfig = ResourceGateConfig {
    capacity: 4,
    warmup_samples: 2,
    rolling_window: 2,
    rss_tolerance_kib: 5,
    open_fd_tolerance: 1,
    open_socket_tolerance: 1,
};

fn samples(values: &[(u64, u64, u64)]) -> Vec<ResourceSample> {
    values
        .iter()
        .enumerate()
        .map(
            |(timestamp_secs, &(rss_kib, open_fds, open_sockets))| ResourceSample {
                timestamp_secs: timestamp_secs as u64,
                rss_kib,
                open_fds,
                open_sockets,
            },
        )
        .collect()
}

fn expected_markdown(
    status: &str,
    finals: (u64, u64, u64),
    verdicts: (&str, &str, &str),
    diagnostic: &str,
) -> String {
    format!(
        "# Rockstream resource-leak soak\n\nstatus: {status}\nsamples: 4/4 (fill: 100%)\nwarmup samples: 2\nrolling window: 2\n\n| resource | unit | baseline | tolerance | final rolling median | slope verdict |\n| --- | --- | ---: | ---: | ---: | --- |\n| RSS | KiB | 100 | 5 | {} | {} |\n| open FD | count | 10 | 1 | {} | {} |\n| open socket | count | 3 | 1 | {} | {} |\n\ndiagnostic: {diagnostic}\n",
        finals.0, verdicts.0, finals.1, verdicts.1, finals.2, verdicts.2,
    )
}

fn expected_json(
    status: &str,
    finals: (u64, u64, u64),
    verdicts: (&str, &str, &str),
    diagnostic: &str,
) -> String {
    format!(
        "{{\"status\":\"{status}\",\"samples\":{{\"collected\":4,\"capacity\":4,\"fill_percent\":100}},\"warmup_samples\":2,\"rolling_window\":2,\"resources\":[{{\"resource\":\"RSS\",\"unit\":\"KiB\",\"baseline\":100,\"tolerance\":5,\"final_rolling_median\":{},\"slope_verdict\":\"{}\"}},{{\"resource\":\"open FD\",\"unit\":\"count\",\"baseline\":10,\"tolerance\":1,\"final_rolling_median\":{},\"slope_verdict\":\"{}\"}},{{\"resource\":\"open socket\",\"unit\":\"count\",\"baseline\":3,\"tolerance\":1,\"final_rolling_median\":{},\"slope_verdict\":\"{}\"}}],\"diagnostic\":\"{diagnostic}\"}}\n",
        finals.0, verdicts.0, finals.1, verdicts.1, finals.2, verdicts.2,
    )
}

fn assert_passing_summary(values: &[(u64, u64, u64)], finals: (u64, u64, u64)) {
    let summary = ResourceSeriesGate::new(CONFIG)
        .evaluate(&samples(values))
        .unwrap();
    let diagnostic = "all rolling medians are within their resource baseline + tolerance";
    assert_eq!(
        summary.render_markdown(),
        expected_markdown("PASS", finals, ("PASS", "PASS", "PASS"), diagnostic)
    );
    assert_eq!(
        summary.render_json(),
        expected_json("PASS", finals, ("PASS", "PASS", "PASS"), diagnostic)
    );
}

fn assert_rejected_series(
    values: &[(u64, u64, u64)],
    finals: (u64, u64, u64),
    verdicts: (&str, &str, &str),
    diagnostic: &str,
) {
    let error = ResourceSeriesGate::new(CONFIG)
        .evaluate(&samples(values))
        .unwrap_err();
    assert_eq!(
        error.render_markdown(),
        expected_markdown("FAIL", finals, verdicts, diagnostic)
    );
    assert_eq!(
        error.render_json(),
        expected_json("FAIL", finals, verdicts, diagnostic)
    );
}

fn assert_error_contract(error: ResourceGateError, diagnostic: &str) {
    assert_eq!(
        error.render_markdown(),
        format!("# Rockstream resource-leak soak\n\nstatus: FAIL\n\ndiagnostic: {diagnostic}\n")
    );
    assert_eq!(
        error.render_json(),
        format!("{{\"status\":\"FAIL\",\"diagnostic\":\"{diagnostic}\"}}\n")
    );
}

#[test]
fn resource_series_accepts_flat_rss_within_band() {
    assert_passing_summary(
        &[(100, 10, 3), (100, 10, 3), (103, 10, 3), (104, 10, 3)],
        (103, 10, 3),
    );
}

#[test]
fn resource_series_accepts_flat_fd_within_band() {
    assert_passing_summary(
        &[(100, 10, 3), (100, 10, 3), (100, 11, 3), (100, 11, 3)],
        (100, 11, 3),
    );
}

#[test]
fn resource_series_accepts_flat_socket_within_band() {
    assert_passing_summary(
        &[(100, 10, 3), (100, 10, 3), (100, 10, 4), (100, 10, 4)],
        (100, 10, 4),
    );
}

#[test]
fn resource_series_rejects_monotonic_rss_leak() {
    assert_rejected_series(
        &[(100, 10, 3), (100, 10, 3), (106, 10, 3), (108, 10, 3)],
        (107, 10, 3),
        ("FAIL", "PASS", "PASS"),
        "RSS rolling median 107 KiB exceeds baseline 100 KiB + tolerance 5 KiB",
    );
}

#[test]
fn resource_series_rejects_monotonic_fd_leak() {
    assert_rejected_series(
        &[(100, 10, 3), (100, 10, 3), (100, 12, 3), (100, 13, 3)],
        (100, 12, 3),
        ("PASS", "FAIL", "PASS"),
        "open FD rolling median 12 count exceeds baseline 10 count + tolerance 1 count",
    );
}

#[test]
fn resource_series_rejects_monotonic_socket_leak() {
    assert_rejected_series(
        &[(100, 10, 3), (100, 10, 3), (100, 10, 5), (100, 10, 6)],
        (100, 10, 5),
        ("PASS", "PASS", "FAIL"),
        "open socket rolling median 5 count exceeds baseline 3 count + tolerance 1 count",
    );
}

#[test]
fn resource_series_refuses_samples_beyond_its_named_capacity() {
    let error = ResourceSeriesGate::new(CONFIG)
        .evaluate(&samples(&[
            (100, 10, 3),
            (100, 10, 3),
            (100, 10, 3),
            (100, 10, 3),
            (100, 10, 3),
        ]))
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "resource soak sample capacity 4 is full; refusing unbounded accumulation"
    );
}

#[test]
fn resource_series_reports_configuration_and_sample_errors_exactly() {
    assert_error_contract(
        ResourceSeriesGate::new(ResourceGateConfig { capacity: 0, ..CONFIG })
            .evaluate(&[])
            .unwrap_err(),
        "invalid resource soak configuration: capacity, warmup_samples, and rolling_window must all be nonzero",
    );
    assert_error_contract(
        ResourceSeriesGate::new(ResourceGateConfig {
            warmup_samples: 3,
            rolling_window: 2,
            ..CONFIG
        })
        .evaluate(&[])
        .unwrap_err(),
        "invalid resource soak configuration: warmup_samples 3 + rolling_window 2 exceeds capacity 4",
    );
    assert_error_contract(
        ResourceSeriesGate::new(CONFIG)
            .evaluate(&samples(&[(100, 10, 3)]))
            .unwrap_err(),
        "resource soak requires 4 samples but collected 1",
    );
}

#[test]
fn resource_sampler_rejects_zero_sample_interval_exactly() {
    let error = match ProcessResourceSampler::new(1, 1, 0) {
        Ok(_) => panic!("zero sample interval must be rejected"),
        Err(error) => error,
    };
    assert_error_contract(
        error,
        "invalid resource soak configuration: sample_interval_secs must be nonzero",
    );
}

#[test]
fn resource_series_artifact_is_the_exact_summary_contract() {
    let summary = ResourceSeriesGate::new(CONFIG)
        .evaluate(&samples(&[
            (100, 10, 3),
            (100, 10, 3),
            (103, 10, 3),
            (104, 10, 3),
        ]))
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let diagnostic = "all rolling medians are within their resource baseline + tolerance";

    summary.write_artifact(directory.path()).unwrap();

    assert_eq!(
        std::fs::read_to_string(directory.path().join("resource-leak-soak-summary.md")).unwrap(),
        expected_markdown("PASS", (103, 10, 3), ("PASS", "PASS", "PASS"), diagnostic),
    );
    assert_eq!(
        std::fs::read_to_string(directory.path().join("resource-leak-soak-summary.json")).unwrap(),
        expected_json("PASS", (103, 10, 3), ("PASS", "PASS", "PASS"), diagnostic),
    );
}
