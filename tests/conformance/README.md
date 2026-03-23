# Conformance Harness

This directory is for tests that check shared specs against expected behavior.

Likely contents later:

- canonical cases tied to spec text
- fixtures for types, null handling, and edge cases
- result snapshots or assertions

Current rule: issue #10 approves the first narrow local conformance slice. Keep canonical cases documented here, and keep executable coverage tightly scoped to that documented projection path until broader harness coverage is approved.

Issue #25 adds the current local fixture expectation for that slice: prefer asserting through `tiforth_kernel::LocalExecutionFixture`, exported from `LocalExecutionSnapshot`, when recording milestone-1 runtime and admission outcomes for local Rust-side coverage.

Issue #27 adds the first checked-in fixture files for that carrier under `tests/conformance/fixtures/local-execution/`. The format and scope for those local JSON artifacts are described in `tests/conformance/local-execution-fixtures.md`.

Issue #139 adds the first post-gate filter conformance case checkpoint in `tests/conformance/first-filter-is-not-null-slice.md`.

Issue #141 adds the signed/unsigned interaction checkpoint in `tests/conformance/signed-unsigned-interaction-checkpoint.md`.

Issue #300 adds the first unsigned arithmetic conformance checkpoint in `tests/conformance/first-unsigned-arithmetic-slice.md`. Issue #308 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`. Issue #310 adds the first executable TiDB/TiFlash differential harness and checked-in artifact coverage for the same slice.

Issue #426 adds the first signed-widening `add<int64>` conformance checkpoint in `tests/conformance/first-signed-widening-add-int64-slice.md`. Executable local kernel coverage for that checkpoint remains follow-on scope.

Issue #176 adds the first temporal `date32` conformance checkpoint in `tests/conformance/first-temporal-date32-slice.md`.

Issue #280 adds the first timezone-aware timestamp `timestamp_tz(us)` conformance checkpoint in `tests/conformance/first-temporal-timestamp-tz-slice.md`. Issue #288 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`.

Issue #189 adds the first decimal `decimal128` conformance checkpoint in `tests/conformance/first-decimal128-slice.md`, and local executable coverage for that checkpoint now exists in `crates/tiforth-kernel/tests/decimal128_slice.rs`.

Issue #194 adds the first float64 NaN/infinity ordering conformance checkpoint in `tests/conformance/first-float64-ordering-slice.md`. Issue #196 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/float64_slice.rs`.

Issue #224 adds the first JSON conformance checkpoint in `tests/conformance/first-json-slice.md`. Issue #354 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/json_slice.rs`. Issue #356 adds first executable TiDB-versus-TiFlash differential adapter and harness coverage for that checkpoint in `crates/tiforth-adapter-tidb/src/first_json_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_json_slice.rs`, and `crates/tiforth-harness-differential/src/first_json_slice.rs`, with checked-in `inventory/first-json-slice-*` artifacts.

Issue #226 adds the first struct passthrough conformance checkpoint in `tests/conformance/first-struct-slice.md`. Issue #329 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/struct_slice.rs`. Issue #360 adds first executable TiDB-versus-TiFlash differential adapter and harness coverage for that checkpoint in `crates/tiforth-adapter-tidb/src/first_struct_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_struct_slice.rs`, and `crates/tiforth-harness-differential/src/first_struct_slice.rs`, with checked-in `inventory/first-struct-slice-*` artifacts.

Issue #230 adds the first map passthrough conformance checkpoint in `tests/conformance/first-map-slice.md`. Issue #334 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/map_slice.rs`. Issue #362 adds first executable TiDB-versus-TiFlash differential adapter and harness coverage for that checkpoint in `crates/tiforth-adapter-tidb/src/first_map_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_map_slice.rs`, and `crates/tiforth-harness-differential/src/first_map_slice.rs`, with checked-in `inventory/first-map-slice-*` artifacts.

Issue #241 adds the first union passthrough conformance checkpoint in `tests/conformance/first-union-slice.md`. Issue #336 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/union_slice.rs`. Issue #366 adds first executable TiDB-versus-TiFlash differential adapter and harness coverage for that checkpoint in `crates/tiforth-adapter-tidb/src/first_union_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_union_slice.rs`, and `crates/tiforth-harness-differential/src/first_union_slice.rs`, with checked-in `inventory/first-union-slice-*` artifacts. Issue #368 adds first executable TiKV single-engine and pairwise differential harness coverage for that checkpoint in `crates/tiforth-adapter-tikv/src/first_union_slice.rs`, `crates/tiforth-harness-differential/src/first_union_slice_tikv.rs`, and `crates/tiforth-harness-differential/src/first_union_slice_tikv_pairwise.rs`, with checked-in TiKV `inventory/first-union-slice-*` artifacts.

Issue #233 adds the first collation-sensitive string conformance checkpoint in `tests/conformance/first-collation-string-slice.md`. Issue #352 adds first executable local conformance coverage for that checkpoint in `crates/tiforth-kernel/tests/collation_string_slice.rs`. Issue #358 adds first executable TiDB-versus-TiFlash differential adapter and harness coverage for that checkpoint in `crates/tiforth-adapter-tidb/src/first_collation_string_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_collation_string_slice.rs`, and `crates/tiforth-harness-differential/src/first_collation_string_slice.rs`, with checked-in `inventory/first-collation-string-slice-*` artifacts.
