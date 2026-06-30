# PostgreSQL Wire Protocol Conformance

This document enumerates every supported PostgreSQL wire message type and SQL
statement class, each with a linked proof test.

The unit test `test_conformance_doc_has_linked_tests` in
`crates/rockstream-gateway/tests/conformance_doc_tests.rs` reads this file,
extracts every `file::function` link, and asserts the named function exists
in the test corpus.

---

## Authentication Messages

| Message | Direction | Proof Test |
|---|---|---|
| `AuthenticationSASL` | B→F | `auth_scram_tests.rs::test_scram_tokio_postgres_connects` |
| `AuthenticationSASLContinue` | B→F | `gateway_proof_tests.rs::test_scram_auth_flow_unit` |
| `AuthenticationSASLFinal` | B→F | `gateway_proof_tests.rs::test_scram_auth_flow_unit` |
| `AuthenticationMD5Password` | B→F | `auth_scram_tests.rs::test_md5_tokio_postgres_connects` |
| `AuthenticationOk` | B→F | `gateway_proof_tests.rs::test_scram_auth_flow_unit` |

---

## Session Startup

| Message | Direction | Proof Test |
|---|---|---|
| `StartupMessage` | F→B | `gateway_integration_tests.rs::server_starts_and_accepts_connection` |
| `SSLRequest` | F→B | `gateway_extended_query_tests.rs::test_ssl_negotiation_downgrade` |
| `ParameterStatus` | B→F | `auth_scram_tests.rs::test_version_banner` |
| `BackendKeyData` | B→F | `gateway_integration_tests.rs::server_starts_and_accepts_connection` |
| `ReadyForQuery` | B→F | `gateway_integration_tests.rs::server_starts_and_accepts_connection` |

---

## Simple Query Protocol

| Message | Direction | Proof Test |
|---|---|---|
| `Query` | F→B | `gateway_integration_tests.rs::server_starts_and_accepts_connection` |
| `CommandComplete` | B→F | `gateway_integration_tests.rs::server_starts_and_accepts_connection` |
| `DataRow` | B→F | `gateway_proof_tests.rs::proof_psql_select_limit_10_under_10ms_p99` |
| `RowDescription` | B→F | `gateway_proof_tests.rs::proof_psql_select_limit_10_under_10ms_p99` |
| `EmptyQueryResponse` | B→F | `gateway_extended_query_tests.rs::test_multi_statement_and_empty_queries` |

---

## Extended Query Protocol

| Message | Direction | Proof Test |
|---|---|---|
| `Parse` | F→B | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `Bind` | F→B | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `Execute` | F→B | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `Describe` | F→B | `gateway_integration_tests.rs::extended_query_protocol_parse_bind_execute` |
| `Close` | F→B | `gateway_extended_query_tests.rs::test_prepared_statement_caching_and_deallocate` |
| `Flush` | F→B | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `Sync` | F→B | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `ParseComplete` | B→F | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `BindComplete` | B→F | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `CloseComplete` | B→F | `gateway_extended_query_tests.rs::test_prepared_statement_caching_and_deallocate` |
| `PortalSuspended` | B→F | `gateway_extended_query_tests.rs::test_portal_suspension_max_rows` |
| `NoData` | B→F | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |
| `ParameterDescription` | B→F | `gateway_extended_query_tests.rs::test_extended_query_pipeline` |

---

## Copy Protocol

| Message | Direction | Proof Test |
|---|---|---|
| `CopyInResponse` | B→F | `gateway_proof_tests.rs::copy_from_stdin_returns_copy_in_response` |
| `CopyOutResponse` | B→F | `gateway_proof_tests.rs::copy_out_streams_view_rows` |
| `CopyData` | B↔F | `gateway_proof_tests.rs::copy_in_basic_rows_visible_lfs` |
| `CopyDone` | F→B | `gateway_proof_tests.rs::copy_in_basic_rows_visible_lfs` |
| `CopyFail` | F→B | `golden_wire_tests.rs::test_golden_wire_copy_in` |

---

## Async / Notifications

| Message | Direction | Proof Test |
|---|---|---|
| `NotificationResponse` | B→F | `listen_notify_tests.rs::test_listen_notify_roundtrip` |
| `NoticeResponse` | B→F | `golden_wire_tests.rs::test_golden_wire_simple_query` |

---

## Errors

| Message | Direction | Proof Test |
|---|---|---|
| `ErrorResponse` | B→F | `gateway_proof_tests.rs::proof_serializable_returns_rs2003` |

---

## Cancellation

| Message | Direction | Proof Test |
|---|---|---|
| `CancelRequest` | F→B | `gateway_proof_tests.rs::test_cancel_request_aborts_query` |

---

## Transaction

| Statement / Feature | Proof Test |
|---|---|
| `BEGIN` | `transaction_savepoint_tests.rs::test_savepoint_rollback_partial_write` |
| `COMMIT` | `transaction_savepoint_tests.rs::test_savepoint_rollback_partial_write` |
| `ROLLBACK` | `gateway_proof_tests.rs::rollback_discards_write_buffer_no_shard_writes` |
| `SAVEPOINT` | `transaction_savepoint_tests.rs::test_savepoint_rollback_partial_write` |
| `RELEASE SAVEPOINT` | `transaction_savepoint_tests.rs::test_savepoint_release_does_not_discard` |
| `ROLLBACK TO SAVEPOINT` | `transaction_savepoint_tests.rs::test_savepoint_rollback_partial_write` |
| Status byte `I` (idle) | `transaction_savepoint_tests.rs::test_tx_status_lifecycle` |
| Status byte `T` (in transaction) | `transaction_savepoint_tests.rs::test_tx_status_lifecycle` |
| Status byte `E` (failed transaction) | `transaction_savepoint_tests.rs::test_tx_status_lifecycle` |

---

## Named Cursors

| Statement | Proof Test |
|---|---|
| `DECLARE CURSOR FOR` | `gateway_proof_tests.rs::test_named_cursor_lifecycle` |
| `FETCH` | `gateway_proof_tests.rs::test_named_cursor_lifecycle` |
| `CLOSE cursor` | `gateway_proof_tests.rs::test_named_cursor_lifecycle` |

---

## Statement Classes

| Statement | Proof Test |
|---|---|
| `SELECT` | `gateway_integration_tests.rs::server_starts_and_accepts_connection` |
| `INSERT` | `gateway_proof_tests.rs::insert_accumulates_in_write_buffer` |
| `UPDATE` | `gateway_dml_tests.rs::test_update_accumulates_in_write_buffer` |
| `DELETE` | `gateway_proof_tests.rs::delete_accumulates_in_write_buffer` |
| `CREATE TABLE` | `gateway_proof_tests.rs::create_table_registers_in_catalog` |
| `CREATE VIEW` | `gateway_proof_tests.rs::proof_inline_view_inlined_into_materialized_view` |
| `CREATE MATERIALIZED VIEW` | `gateway_proof_tests.rs::proof_inline_view_inlined_into_materialized_view` |
| `REFRESH MATERIALIZED VIEW` | `gateway_dml_tests.rs::test_refresh_materialized_view_roundtrip` |
| `LISTEN` | `listen_notify_tests.rs::test_listen_notify_roundtrip` |
| `NOTIFY` | `listen_notify_tests.rs::test_listen_notify_roundtrip` |
| `UNLISTEN` | `listen_notify_tests.rs::test_unlisten_stops_delivery` |
| `SUBSCRIBE` | `gateway_proof_tests.rs::proof_subscribe_snapshot_then_deltas_lfs` |
| `SET` | `auth_scram_tests.rs::test_set_show_search_path` |
| `SHOW` | `auth_scram_tests.rs::test_set_show_search_path` |
