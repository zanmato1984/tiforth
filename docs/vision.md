# Vision

## Goals

- Unify relational operators and expression or function behavior across TiDB, TiFlash, and TiKV.
- Establish clear data and runtime contracts before implementation work.
- Build harnesses that can validate semantics, surface drift, and measure performance across engines.
- Reuse donor ideas that still help: Arrow for data interchange and broken-pipeline for runtime direction.

## Non-Goals

- Porting the legacy repository as-is.
- Carrying forward operator or function implementations that are already considered poor fits.
- Locking every unresolved semantic choice before inventory and harness work starts.
- Choosing a permanent language toolchain in this skeleton commit.

## Why Harness-First

Harness-first forces the repository to answer a practical question early: how will correctness and drift be measured? That matters more than shipping placeholder kernels.

It also keeps the reboot honest:

- specs can be tested against real engines
- disagreements can be recorded before they become code paths
- later implementation work can target a stable contract instead of a moving guess

## Immediate Priorities

- inventory donor and engine behavior without inheriting donor code
- define minimal data and runtime contracts
- stand up conformance, differential, and performance harness boundaries

## Current Checkpoints

- the first thin end-to-end slice is the local source -> projection -> sink path documented in `docs/spec/milestone-1-expression-projection.md`
- the first accepted kernel boundary is the milestone-1 expression-projection slice backed by the data and runtime contracts plus local conformance coverage
- the first documented differential checkpoint is the TiDB-versus-TiFlash expression slice in `tests/differential/first-expression-slice.md`
- the first executable differential checkpoint now exists through `crates/tiforth-harness-differential` and checked-in paired artifacts under `inventory/`
- the first executable TiKV single-engine adapter checkpoint for `first-expression-slice` now exists through `crates/tiforth-adapter-tikv`, with request/response scope fixed in `adapters/first-expression-slice-tikv.md`
- the first checked-in TiKV single-engine normalized `case-results` artifact for `first-expression-slice` now exists at `inventory/first-expression-slice-tikv-case-results.json`
- the first executable TiKV pairwise drift checkpoint for `first-expression-slice` now exists through `crates/tiforth-harness-differential/src/first_expression_slice_tikv_pairwise.rs` and checked-in paired artifacts at `inventory/first-expression-slice-tidb-vs-tikv-drift-report.{md,json}` plus `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.{md,json}`
- the first machine-readable drift-report sidecars now exist for both milestone-1 differential slices under `inventory/` alongside their required Markdown drift reports
- the first checked-in per-engine compatibility notes now cover first-expression, first-filter, first-temporal-date32, first-temporal-timestamp-tz, first-decimal128, first-float64-ordering, and first-unsigned-arithmetic for TiDB, TiFlash, and TiKV under `inventory/`
- the first post-gate shared-kernel expansion candidate is now fixed in `docs/design/first-post-gate-kernel-boundary.md`
- the docs-first filter prep checkpoint now covers semantics, conformance, differential slice shape, and adapter boundary through `docs/spec/first-filter-is-not-null.md`, `tests/conformance/first-filter-is-not-null-slice.md`, `tests/differential/first-filter-is-not-null-slice.md`, and `adapters/first-filter-is-not-null-slice.md`
- the first executable TiKV single-engine filter checkpoint now exists through `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv.rs`, and checked-in artifacts `inventory/first-filter-is-not-null-slice-tikv-case-results.json` plus `inventory/first-filter-is-not-null-slice-tikv-compat-notes.md`
- the first executable TiKV pairwise filter drift checkpoint now exists through `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv_pairwise.rs` and checked-in paired artifacts at `inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.{md,json}` plus `inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.{md,json}`
- the first unsigned arithmetic checkpoint now has docs-first semantic, conformance, differential, and adapter anchors through `docs/design/first-unsigned-arithmetic-slice.md`, `tests/conformance/first-unsigned-arithmetic-slice.md`, `tests/differential/first-unsigned-arithmetic-slice.md`, `tests/differential/first-unsigned-arithmetic-slice-artifacts.md`, `adapters/first-unsigned-arithmetic-slice.md`, and `adapters/first-unsigned-arithmetic-slice-tikv.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`, executable TiDB-versus-TiFlash differential harness coverage in `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice.rs`, and executable TiKV single-engine plus pairwise harness coverage in `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice_tikv.rs`, and `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice_tikv_pairwise.rs` with checked-in artifacts under `inventory/`
- the first checked-in TiDB, TiFlash, and TiKV unsigned arithmetic compatibility notes now exist at `inventory/first-unsigned-arithmetic-slice-tidb-compat-notes.md`, `inventory/first-unsigned-arithmetic-slice-tiflash-compat-notes.md`, and `inventory/first-unsigned-arithmetic-slice-tikv-compat-notes.md`, alongside TiKV single-engine and pairwise unsigned drift-report artifacts
- the first temporal date32 checkpoint now has docs and local executable conformance coverage through `docs/design/first-temporal-semantic-slice.md`, `tests/conformance/first-temporal-date32-slice.md`, and `crates/tiforth-kernel/tests/temporal_date32_slice.rs`, with differential and adapter anchors in `tests/differential/first-temporal-date32-slice.md` and `adapters/first-temporal-date32-slice.md`
- the first timezone-aware timestamp checkpoint now has docs-first semantic, conformance, differential, and adapter anchors through `docs/design/first-temporal-timestamp-tz-slice.md`, `tests/conformance/first-temporal-timestamp-tz-slice.md`, `tests/differential/first-temporal-timestamp-tz-slice.md`, and `adapters/first-temporal-timestamp-tz-slice.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/temporal_timestamp_tz_slice.rs`, executable TiDB-versus-TiFlash differential harness coverage in `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice.rs`, and executable TiKV single-engine plus pairwise harness coverage in `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv.rs`, and `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv_pairwise.rs` with checked-in artifacts under `inventory/`
- the first checked-in timestamp-timezone TiKV compatibility notes checkpoint now exists at `inventory/first-temporal-timestamp-tz-slice-tikv-compat-notes.md` alongside TiKV single-engine and pairwise drift-report artifacts
- the first executable TiKV single-engine temporal date32 checkpoint now exists through `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv.rs`, and checked-in artifacts `inventory/first-temporal-date32-slice-tikv-case-results.json` plus `inventory/first-temporal-date32-slice-tikv-compat-notes.md`
- the first executable TiKV pairwise temporal date32 drift checkpoint now exists through `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_pairwise.rs` and checked-in paired artifacts at `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.{md,json}` plus `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.{md,json}`
- the first decimal `decimal128` checkpoint now has docs, local executable conformance coverage, and executable differential artifacts through `docs/design/first-decimal-semantic-slice.md`, `tests/conformance/first-decimal128-slice.md`, `crates/tiforth-kernel/tests/decimal128_slice.rs`, `crates/tiforth-harness-differential/src/first_decimal128_slice.rs`, `tests/differential/first-decimal128-slice-artifacts.md`, and checked-in paired first-decimal artifacts under `inventory/`
- the first executable TiKV single-engine and pairwise decimal `decimal128` checkpoints now exist through `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv.rs`, and `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv_pairwise.rs`, with checked-in artifacts at `inventory/first-decimal128-slice-tikv-case-results.json`, `inventory/first-decimal128-slice-tikv-compat-notes.md`, `inventory/first-decimal128-slice-tidb-vs-tikv-drift-report.{md,json}`, and `inventory/first-decimal128-slice-tiflash-vs-tikv-drift-report.{md,json}`
- the first float64 NaN/infinity ordering checkpoint now has docs anchors, local executable conformance coverage, and executable differential artifacts through `docs/design/first-float64-ordering-slice.md`, `tests/conformance/first-float64-ordering-slice.md`, `crates/tiforth-kernel/tests/float64_slice.rs`, `crates/tiforth-harness-differential/src/first_float64_ordering_slice.rs`, `tests/differential/first-float64-ordering-slice-artifacts.md`, and checked-in paired first-float64-ordering artifacts under `inventory/`
- the first executable TiKV single-engine and pairwise float64 ordering checkpoints now exist through `crates/tiforth-adapter-tikv`, `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv.rs`, and `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv_pairwise.rs`, with checked-in artifacts `inventory/first-float64-ordering-slice-tikv-case-results.json`, `inventory/first-float64-ordering-slice-tikv-compat-notes.md`, `inventory/first-float64-ordering-slice-tidb-vs-tikv-drift-report.{md,json}`, and `inventory/first-float64-ordering-slice-tiflash-vs-tikv-drift-report.{md,json}`
- the first JSON comparability/cast checkpoint now has docs-first semantic, conformance, differential, adapter, and artifact-carrier anchors through `docs/design/first-json-semantic-slice.md`, `tests/conformance/first-json-slice.md`, `tests/differential/first-json-slice.md`, `tests/differential/first-json-slice-artifacts.md`, and `adapters/first-json-slice.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/json_slice.rs`, executable TiDB-versus-TiFlash adapter and harness coverage in `crates/tiforth-adapter-tidb/src/first_json_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_json_slice.rs`, and `crates/tiforth-harness-differential/src/first_json_slice.rs`, with checked-in artifacts under `inventory/first-json-slice-*`
- the first collation-sensitive string checkpoint now has docs-first semantic, conformance, differential, adapter, and artifact-carrier anchors through `docs/design/first-collation-string-slice.md`, `tests/conformance/first-collation-string-slice.md`, `tests/differential/first-collation-string-slice.md`, `tests/differential/first-collation-string-slice-artifacts.md`, and `adapters/first-collation-string-slice.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/collation_string_slice.rs`
- the first nested struct handoff checkpoint now has docs-first semantic, conformance, differential, adapter, and artifact-carrier anchors through `docs/design/first-struct-aware-handoff-slice.md`, `tests/conformance/first-struct-slice.md`, `tests/differential/first-struct-slice.md`, `tests/differential/first-struct-slice-artifacts.md`, and `adapters/first-struct-slice.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/struct_slice.rs`
- the first nested map handoff checkpoint now has docs-first semantic, conformance, differential, adapter, and artifact-carrier anchors through `docs/design/first-map-aware-handoff-slice.md`, `tests/conformance/first-map-slice.md`, `tests/differential/first-map-slice.md`, `tests/differential/first-map-slice-artifacts.md`, and `adapters/first-map-slice.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/map_slice.rs`
- the first nested union handoff checkpoint now has docs-first semantic, conformance, differential, adapter, and artifact-carrier anchors through `docs/design/first-union-aware-handoff-slice.md`, `tests/conformance/first-union-slice.md`, `tests/differential/first-union-slice.md`, `tests/differential/first-union-slice-artifacts.md`, and `adapters/first-union-slice.md`, plus local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/union_slice.rs`
- the first executable differential temporal date32 checkpoint now exists through `crates/tiforth-harness-differential` and checked-in paired first-temporal artifacts under `inventory/`
- the first executable post-gate filter checkpoint now exists through `crates/tiforth-kernel` via `FilterPipe` and executable coverage in `crates/tiforth-kernel/tests/filter_is_not_null.rs`
- the first executable differential filter checkpoint now exists through `crates/tiforth-harness-differential` and checked-in paired first-filter artifacts under `inventory/`
- the first executable differential exchange-parity checkpoint now exists through `crates/tiforth-harness-differential/src/first_exchange_slice.rs` over the existing `first-expression-slice` and `first-filter-is-not-null-slice` case IDs
- the first exchange parity artifact-carrier checkpoint now defines baseline-versus-exchange markdown and JSON drift-report carriers through `tests/differential/first-exchange-slice-artifacts.md`
- the first live-runner orchestration path for `first-filter-is-not-null-slice` now exists through `crates/tiforth-harness-differential/src/first_filter_is_not_null_live.rs` and `crates/tiforth-harness-differential/src/bin/first_filter_is_not_null_live.rs`

## Next Checkpoint

- the next checkpoint is to execute `first-collation-string-slice` differential adapter and harness coverage so the documented `case_id`, `input_ref`, `collation_ref`, and artifact carriers land as checked-in inventory evidence
- that follow-on checkpoint should keep the current `case_id`, `input_ref`, `collation_ref`, and artifact-carrier identifiers stable before broadening locale families or adding shared runtime collation negotiation state

## Kernel Expansion Gate

- widening the shared kernel beyond the current milestone-1 slice now requires the acceptance gate in `docs/design/kernel-expansion-acceptance.md`
- that gate requires executable differential evidence from the next thin slice plus docs-first scope, named harness coverage, and concrete completion evidence for any later kernel expansion

## First Inventory Wave

- the first inventory wave is documented in `docs/design/first-inventory-wave.md`
- that wave stays aligned with the milestone-1 projection semantic core: the projection operator plus `column`, `literal<int32>`, and `add<int32>` expression families
- donor catalogs, engine compatibility notes, and any first coverage-gap inventory should anchor to the stable refs already defined for `tests/differential/first-expression-slice.md`
