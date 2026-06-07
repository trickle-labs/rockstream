# E2E Test Suite Improvement Plan

## Overview
This plan outlines improvements for the `rockstream-e2e` test suite to improve coverage, maintainability, and reliability.

## 🚀 Structure & Readability
### Split Large Files
- Currently, `crates/rockstream-e2e/tests/e2e.rs` is very large. We will split it into:
  - `cli_tests.rs`: CLI help and snapshot tests.
  - `role_matrix_tests.rs`: Role startup and matrix verification.
  - `bootstrap_tests.rs`: Bootstrap flow (success/failure).
  - `query_coverage_tests.rs`: Comprehensive query, DDL/DML, and catalog coverage.

### Abstract Common Setup
- Create a shared fixture for starting the RockStream image and ensuring it is built to remove boilerplate from every test function.

## 🐞 Coverage & Reliability
### Robust Polling
- Replace `toktio::time::sleep` calls with polling mechanisms that check for:
  - Port availability (e.g., using `wait_for_port`).
  - Service health (e.g., checking `/health` endpoints).

### Error Code Coverage
- Ensure all standard RS-xxxx error codes are covered, especially those related to query planning and catalog reflection.

### Query Variations
- Add tests for `INSERT ... RETURNING`.
- Expand view dependency checks for more complex schemas.

## ✨ Utility Improvements
### Result Parsing Helper
- Create a unified helper that takes an `expected_outcome` (e.g., ErrorCode) and verifies the `docker logs` output, reducing manual string matching in every test.
