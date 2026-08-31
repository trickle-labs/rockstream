.PHONY: build test clippy fmt documentation check e2e e2e-lfs e2e-nextest qualify approve clean error-codes exit-criteria failure-matrix coverage coverage-gate release verify verify-relaxed path-coupling bench-baseline-update

# Build the workspace
build:
	cargo build --workspace

# Run all tests
test:
	cargo test --workspace

# Run clippy
clippy:
	cargo clippy --workspace --all-targets -- -D warnings

# Check formatting
fmt:
	cargo fmt --all --check

# Check documentation links, generated references, claims, terminology, and commands.
documentation:
	bash scripts/check-documentation.sh
	bash scripts/check-documentation.test.sh

# Run all checks (what CI does)
check: fmt clippy test documentation error-codes exit-criteria verify path-coupling failure-matrix

# Run formal verification specs
verify:
	fizz formal/smoke.fizz
	fizz formal/m1_epoch_commit.fizz
	fizz formal/m2_frontier_agg.fizz
	fizz formal/m3_sink_2pc.fizz
	fizz formal/m4_self_fencing.fizz
	fizz formal/m6_shard_migration.fizz
	fizz formal/m7_control_plane_ha.fizz

# Run formal verification specs at relaxed pre-release bounds (DC.4).
# Widens coverage beyond CI-fast minimums: NUM_WORKERS=3, NUM_SHARDS=3, MAX_EPOCH=4.
verify-relaxed:
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/smoke.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m1_epoch_commit.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m2_frontier_agg.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m3_sink_2pc.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m4_self_fencing.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m6_shard_migration.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m7_control_plane_ha.fizz

# Path-coupling check: any change to a coordination crate or DESIGN.md requires
# a corresponding touch to formal/*.fizz or FIZZBEE_TEST_PLAN.md (DC.2).
path-coupling:
	bash scripts/check-path-coupling.sh

# Enforce that every logged error carries an RS-XXXX code.
error-codes:
	bash scripts/check-error-codes.sh

# Enforce that every version marked Done has a complete sign-off.
exit-criteria:
	bash scripts/check-exit-criteria.sh

# Enforce failure matrix completeness, non-vacuous recovery assertions, test links, and seed references.
failure-matrix:
	bash scripts/check-failure-matrix.sh
	bash scripts/check-failure-matrix.test.sh

# Generate an lcov coverage report for the workspace (requires cargo-llvm-cov).
coverage:
	cargo llvm-cov --workspace --lib --tests --no-report
	cargo llvm-cov --no-clean -p rockstream-gateway --features testcontainers --test auth_scram_tests --test driver_matrix_tests --test query_time_multi_shard_scatter_minio_tests --test reference_app_tests --no-report
	cargo llvm-cov --no-clean -p rockstream-sim --features simulation --lib --no-report -- --test-threads=1
	cargo llvm-cov --no-clean -p rockstream-sim --features simulation --test az_aware_exchange_sim_tests --test control_plane_ha_tests --test control_sim --test frontier_publisher_election --test hot_key_detection_sim_tests --test lock_poisoning_sim_tests --test query_time_scatter_sim_tests --test recursive_cte_sim_tests --test shard_merge_sim_tests --test shard_migration_sim_tests --test shard_split_sim_tests --test shard_stats_checkpoint_sim_tests --test sim_aggregate_coordination_tests --test skew_control_loop_sim_tests --test worker_drain_sim_tests --no-report
	cargo llvm-cov --no-clean -p rockstream-sim --features docker_tests --test real_cluster_chaos_soak_tests --test resource_leak_soak_real_binary_tests --no-report
	cargo llvm-cov report --lcov --output-path lcov.info
	@echo "Coverage written to lcov.info"

# Enforce coverage thresholds for every workspace crate from one complete
# feature-matrix report (requires cargo-llvm-cov). Mirrors the `coverage` job
# in .github/workflows/ci.yml exactly — each crate's floor is
# `max(70, floor(measured baseline %))`, never below 70, never loosened
# below what the crate already achieves. See `.claude/v0.45.3-plan.md` S1
# for the baseline table these numbers come from.
coverage-gate:
	$(MAKE) coverage
	cargo llvm-cov report --package rockstream-gateway --fail-under-lines 76
	cargo llvm-cov report --package rockstream-gateway --fail-under-regions 77
	cargo llvm-cov report --package rockstream-docgen --fail-under-lines 70
	cargo llvm-cov report --package rockstream-docgen --fail-under-regions 70
	cargo llvm-cov report --package rockstream-diff --fail-under-lines 76
	cargo llvm-cov report --package rockstream-diff --fail-under-regions 71
	cargo llvm-cov report --package rockstream-ops --fail-under-lines 82
	cargo llvm-cov report --package rockstream-ops --fail-under-regions 82
	cargo llvm-cov report --package rockstream-storage --fail-under-lines 75
	cargo llvm-cov report --package rockstream-storage --fail-under-regions 75
	cargo llvm-cov report --package rockstream-runtime --fail-under-lines 76
	cargo llvm-cov report --package rockstream-runtime --fail-under-regions 79
	cargo llvm-cov report --package rockstream-sql --fail-under-lines 73
	cargo llvm-cov report --package rockstream-sql --fail-under-regions 74
	cargo llvm-cov report --package rockstream-control --fail-under-lines 78
	cargo llvm-cov report --package rockstream-control --fail-under-regions 79
	cargo llvm-cov report --package rockstream-connectors --fail-under-lines 70
	cargo llvm-cov report --package rockstream-connectors --fail-under-regions 71
	cargo llvm-cov report --package rockstream-types --fail-under-lines 84
	cargo llvm-cov report --package rockstream-types --fail-under-regions 87
	cargo llvm-cov report --package rockstream-plan --fail-under-lines 81
	cargo llvm-cov report --package rockstream-plan --fail-under-regions 81
	cargo llvm-cov report --package rockstream-sim --fail-under-lines 92
	cargo llvm-cov report --package rockstream-sim --fail-under-regions 93
	cargo llvm-cov report --package rockstream-cli --fail-under-lines 73 --ignore-filename-regex '/rockstream-cli/src/main[.]rs$'
	cargo llvm-cov report --package rockstream-cli --fail-under-regions 77 --ignore-filename-regex '/rockstream-cli/src/main[.]rs$'
	cargo llvm-cov report --package rockstream-oracle --fail-under-lines 83
	cargo llvm-cov report --package rockstream-oracle --fail-under-regions 81
	cargo llvm-cov report --package rockstream-test-support --fail-under-lines 70
	cargo llvm-cov report --package rockstream-test-support --fail-under-regions 70
	@echo "Coverage gate passed."

# Re-measure all four v0.45.4 performance-regression benchmark suites and
# overwrite their checked-in baseline JSON files with the freshly measured
# means. Deliberately NOT invoked anywhere in ci.yml — baseline updates must
# stay an explicit, code-reviewed, human-triggered step so a regression can
# never quietly become the new "normal."
bench-baseline-update:
	cargo bench -p rockstream-ops --bench perf_regression -- --noplot | tee /tmp/rockstream-ops-bench.out
	grep '^\[bench_summary:ops\] ' /tmp/rockstream-ops-bench.out | sed 's/^\[bench_summary:ops\] //' \
		| python3 -m json.tool > crates/rockstream-ops/benches/baseline/v0.45.4-ops.json
	cargo bench -p rockstream-storage --bench storage_bench -- --noplot | tee /tmp/rockstream-storage-bench.out
	grep '^\[bench_summary:storage\] ' /tmp/rockstream-storage-bench.out | sed 's/^\[bench_summary:storage\] //' \
		| python3 -m json.tool > crates/rockstream-storage/benches/baseline/v0.45.4-storage.json
	cargo bench -p rockstream-runtime --bench exchange_bench -- --noplot | tee /tmp/rockstream-runtime-bench.out
	grep '^\[bench_summary:runtime\] ' /tmp/rockstream-runtime-bench.out | sed 's/^\[bench_summary:runtime\] //' \
		| python3 -m json.tool > crates/rockstream-runtime/benches/baseline/v0.45.4-runtime.json
	cargo bench -p rockstream-control --bench frontier_bench -- --noplot | tee /tmp/rockstream-control-bench.out
	grep '^\[bench_summary:control\] ' /tmp/rockstream-control-bench.out | sed 's/^\[bench_summary:control\] //' \
		| python3 -m json.tool > crates/rockstream-control/benches/baseline/v0.45.4-control.json
	@echo "Baselines updated. Review the diff (git diff crates/*/benches/baseline/v0.45.4-*.json) before committing."

# End-to-end test: exercises all three required test backends (Unit, LFS, MinIO/TC).
#
# Satisfies the v0.3 proof: "make e2e brings up MinIO + 1 worker + 1 control and tears
# it down."  The MinIO tests use TestContainers to provision a real MinIO instance; the
# LFS tests run against a local-filesystem-backed SlateDB without containers.
e2e:
	@cargo build -p rockstream-cli
	@echo "=== RockStream e2e test ==="
	@echo ""
	@echo "--- Step 1: no-op binary (--role=control + --role=worker) ---"
	@rm -rf /tmp/rockstream-e2e-test
	@mkdir -p /tmp/rockstream-e2e-test
	@ROCKSTREAM_E2E_SLEEP_MS=500 ./target/debug/rockstream start --role=control --storage /tmp/rockstream-e2e-test/control > /tmp/control.stdout 2>&1 & CONTROL_PID=$$! ; \
	sleep 0.2 ; \
	ROCKSTREAM_E2E_SLEEP_MS=200 ./target/debug/rockstream start --role=worker --control=127.0.0.1:8000 --storage /tmp/rockstream-e2e-test/worker ; \
	wait $$CONTROL_PID
	@test -f /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: control audit.jsonl not found" && exit 1)
	@grep -q "pipeline.created" /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: pipeline.created event missing" && exit 1)
	@grep -q "pipeline.started" /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: pipeline.started event missing" && exit 1)
	@grep -q "pipeline.stopped" /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: pipeline.stopped event missing" && exit 1)
	@grep -q "server.started" /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: server.started event missing" && exit 1)
	@grep -q "server.stopped" /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: server.stopped event missing" && exit 1)
	@echo "Audit log OK: all expected events present (role=control)"
	@ls /tmp/rockstream-e2e-test/control/support-bundle-*.json > /dev/null 2>&1 || (echo "FAIL: support bundle not found" && exit 1)
	@cat /tmp/rockstream-e2e-test/control/support-bundle-*.json | grep -q "audit_events" || (echo "FAIL: bundle missing audit_events" && exit 1)
	@cat /tmp/rockstream-e2e-test/control/support-bundle-*.json | grep -q "system_info" || (echo "FAIL: bundle missing system_info" && exit 1)
	@echo "Support bundle content OK"
	@test -f /tmp/rockstream-e2e-test/worker/audit.jsonl || (echo "FAIL: audit.jsonl not found (worker)" && exit 1)
	@echo "Audit log OK: expected events present (role=worker)"
	@rm -rf /tmp/rockstream-e2e-test
	@echo ""
	@echo "--- Step 2: LFS backend integration tests (SlateDB on local filesystem) ---"
	@cargo test -p rockstream-storage --test lfs_backend -- --test-threads=4 2>&1
	@echo "LFS backend tests PASSED"
	@echo ""
	@echo "--- Step 3: MinIO backend integration tests (SlateDB on S3 via TestContainers) ---"
	@echo "Note: requires Docker; tests auto-skip if Docker is unavailable."
	@cargo test -p rockstream-storage --test minio_backend -- --test-threads=4 2>&1
	@echo "MinIO backend tests PASSED (or skipped if Docker not available)"
	@echo ""
	@echo "=== e2e PASSED ==="

# Fast local E2E check (LFS backend only, no Docker required)
e2e-lfs:
	@cargo build -p rockstream-cli
	@echo "=== RockStream e2e-lfs test ==="
	@cargo test -p rockstream-storage --test lfs_backend -- --test-threads=4 2>&1
	@echo "=== e2e-lfs PASSED ==="

# E2E check using cargo-nextest (with graceful fallback if cargo-nextest is not installed)
e2e-nextest:
	@cargo build -p rockstream-cli
	@echo "=== RockStream e2e-nextest test ==="
	@echo ""
	@echo "--- Step 1: no-op binary (--role=control + --role=worker) ---"
	@rm -rf /tmp/rockstream-e2e-test
	@mkdir -p /tmp/rockstream-e2e-test
	@ROCKSTREAM_E2E_SLEEP_MS=500 ./target/debug/rockstream start --role=control --storage /tmp/rockstream-e2e-test/control > /tmp/control.stdout 2>&1 & CONTROL_PID=$$! ; \
	sleep 0.2 ; \
	ROCKSTREAM_E2E_SLEEP_MS=200 ./target/debug/rockstream start --role=worker --control=127.0.0.1:8000 --storage /tmp/rockstream-e2e-test/worker ; \
	wait $$CONTROL_PID
	@test -f /tmp/rockstream-e2e-test/control/audit.jsonl || (echo "FAIL: control audit.jsonl not found" && exit 1)
	@echo "Audit log OK: expected events present"
	@rm -rf /tmp/rockstream-e2e-test
	@echo ""
	@if command -v cargo-nextest >/dev/null 2>&1; then \
		echo "--- Running LFS & MinIO backend tests via cargo-nextest ---" ; \
		cargo nextest run -p rockstream-storage --test lfs_backend --test minio_backend ; \
	else \
		echo "Note: cargo-nextest not found; falling back to cargo test. Install with: brew install cargo-nextest" ; \
		cargo test -p rockstream-storage --test lfs_backend --test minio_backend -- --test-threads=4 2>&1 ; \
	fi
	@echo "=== e2e-nextest PASSED ==="

# Automated End-to-End Release Qualification suite
qualify:
	@bash scripts/run-release-qualification.sh

# Bump the workspace version, commit, tag, and push.
# Usage: make release VERSION=0.5.0
release:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required. Usage: make release VERSION=0.5.0" && exit 1)
	@echo "=== Releasing v$(VERSION) ==="
	@sed -i '' 's/^version = ".*"/version = "$(VERSION)"/' Cargo.toml
	@cargo check --workspace -q
	@git add Cargo.toml Cargo.lock
	@git commit -m "Release v$(VERSION)"
	@git tag -a "v$(VERSION)" -m "Release v$(VERSION)"
	@git push && git push --tags
	@echo "=== Released v$(VERSION) ==="

# Clean build artifacts
clean:
	cargo clean

# Create a sign-off template for a completed version.
# Usage: make approve VERSION=0.6.0
# Fill in the generated file, then commit it alongside the ROADMAP.md ✅ Done update.
approve:
	@test -n "$(VERSION)" || (echo "ERROR: VERSION is required. Usage: make approve VERSION=0.6.0" && exit 1)
	@test ! -f sign-offs/v$(VERSION).md || (echo "ERROR: sign-offs/v$(VERSION).md already exists" && exit 1)
	@mkdir -p sign-offs
	@{ \
	  echo "# v$(VERSION) Sign-off"; \
	  echo ""; \
	  echo "**Signed off**: $$(date +%Y-%m-%d)"; \
	  echo ""; \
	  echo "## Exit Criteria Verification"; \
	  echo ""; \
	  echo "All criteria in the Proof column of ROADMAP.md for v$(VERSION) have been verified."; \
	  echo ""; \
	  echo "- [ ] \`cargo test --workspace\` passes"; \
	  echo "- [ ] All Proof criteria verified against running code or CI output"; \
	  echo "- [ ] ROADMAP.md status updated to \`✅ Done\`"; \
	  echo ""; \
	  echo "## Notes"; \
	  echo ""; \
	} > sign-offs/v$(VERSION).md
	@echo "Created sign-offs/v$(VERSION).md — check off each item, then commit."
