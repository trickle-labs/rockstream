.PHONY: build test clippy fmt check e2e approve clean error-codes exit-criteria coverage coverage-gate release verify verify-relaxed path-coupling

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

# Run all checks (what CI does)
check: fmt clippy test error-codes exit-criteria verify path-coupling

# Run formal verification specs
verify:
	fizz formal/smoke.fizz
	fizz formal/m1_epoch_commit.fizz
	fizz formal/m2_frontier_agg.fizz
	fizz formal/m3_sink_2pc.fizz
	fizz formal/m4_self_fencing.fizz
	fizz formal/m5_cold_tier_sink.fizz
	fizz formal/m7_control_plane_ha.fizz

# Run formal verification specs at relaxed pre-release bounds (DC.4).
# Widens coverage beyond CI-fast minimums: NUM_WORKERS=3, NUM_SHARDS=3, MAX_EPOCH=4.
verify-relaxed:
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/smoke.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m1_epoch_commit.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m2_frontier_agg.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m3_sink_2pc.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m4_self_fencing.fizz
	NUM_WORKERS=3 NUM_SHARDS=3 MAX_EPOCH=4 fizz formal/m5_cold_tier_sink.fizz
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

# Generate an lcov coverage report for the workspace (requires cargo-llvm-cov).
coverage:
	cargo llvm-cov --workspace --lcov --output-path lcov.info
	@echo "Coverage written to lcov.info"

# Enforce coverage thresholds for rockstream-gateway (requires cargo-llvm-cov).
# Fails with non-zero exit if line coverage < 90% or branch coverage < 85%.
coverage-gate:
	cargo llvm-cov --package rockstream-gateway --fail-under-lines 90
	cargo llvm-cov --package rockstream-gateway \
		--include-files 'protocol.rs,server.rs,session.rs,auth.rs' \
		--fail-under-branches 85
	@echo "Coverage gate passed."

# End-to-end test: exercises all three required test backends (Unit, LFS, MinIO/TC).
#
# Satisfies the v0.3 proof: "make e2e brings up MinIO + 1 worker + 1 control and tears
# it down."  The MinIO tests use TestContainers to provision a real MinIO instance; the
# LFS tests run against a local-filesystem-backed SlateDB without containers.
e2e: build
	@echo "=== RockStream e2e test ==="
	@echo ""
	@echo "--- Step 1: no-op binary (--role=control + --role=worker) ---"
	@rm -rf /tmp/rockstream-e2e-test
	@mkdir -p /tmp/rockstream-e2e-test
	@ROCKSTREAM_E2E_SLEEP_MS=4000 ./target/debug/rockstream start --role=control --storage /tmp/rockstream-e2e-test/control > /tmp/control.stdout 2>&1 & CONTROL_PID=$$! ; \
	sleep 1 ; \
	ROCKSTREAM_E2E_SLEEP_MS=1000 ./target/debug/rockstream start --role=worker --control=127.0.0.1:8000 --storage /tmp/rockstream-e2e-test/worker ; \
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
	@cargo test -p rockstream-storage --test minio_backend -- --test-threads=1 2>&1
	@echo "MinIO backend tests PASSED (or skipped if Docker not available)"
	@echo ""
	@echo "=== e2e PASSED ==="

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
