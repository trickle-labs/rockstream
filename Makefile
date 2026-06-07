.PHONY: build test clippy fmt check e2e approve clean error-codes exit-criteria coverage release

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
check: fmt clippy test error-codes exit-criteria

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

# End-to-end test: start a local process, run a no-op pipeline, verify artifacts.
e2e: build
	@echo "=== RockStream e2e test ==="
	@rm -rf /tmp/rockstream-e2e-test
	@cargo run -- start --storage /tmp/rockstream-e2e-test
	@echo ""
	@echo "--- Verifying audit log ---"
	@test -f /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: audit.jsonl not found" && exit 1)
	@grep -q "pipeline.created" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: pipeline.created event missing" && exit 1)
	@grep -q "pipeline.started" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: pipeline.started event missing" && exit 1)
	@grep -q "pipeline.stopped" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: pipeline.stopped event missing" && exit 1)
	@grep -q "server.started" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: server.started event missing" && exit 1)
	@grep -q "server.stopped" /tmp/rockstream-e2e-test/audit.jsonl || (echo "FAIL: server.stopped event missing" && exit 1)
	@echo "Audit log OK: all expected events present"
	@echo ""
	@echo "--- Verifying support bundle ---"
	@ls /tmp/rockstream-e2e-test/support-bundle-*.json > /dev/null 2>&1 || (echo "FAIL: support bundle not found" && exit 1)
	@echo "Support bundle OK: file exists"
	@echo ""
	@echo "--- Verifying support bundle content ---"
	@cat /tmp/rockstream-e2e-test/support-bundle-*.json | grep -q "audit_events" || (echo "FAIL: bundle missing audit_events" && exit 1)
	@cat /tmp/rockstream-e2e-test/support-bundle-*.json | grep -q "system_info" || (echo "FAIL: bundle missing system_info" && exit 1)
	@echo "Support bundle content OK"
	@echo ""
	@echo "=== e2e PASSED ==="
	@rm -rf /tmp/rockstream-e2e-test

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
