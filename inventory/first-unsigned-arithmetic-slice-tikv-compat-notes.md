# First Unsigned Arithmetic Slice TiKV Compatibility Notes

Status: issue #328 adapter, harness, and inventory checkpoint

Verified: 2026-03-20

Related issues:

- #300 `design: define first unsigned arithmetic semantic slice boundary`
- #302 `docs: define first-unsigned-arithmetic-slice differential artifact carriers`
- #310 `milestone-1: execute first unsigned arithmetic differential slice`
- #324 `design: define TiKV adapter boundary for first-unsigned-arithmetic-slice`
- #328 `harness: add TiKV unsigned arithmetic executable checkpoints`

## Purpose

This note records TiKV-side compatibility evidence for the
`first-unsigned-arithmetic-slice` checkpoint.

It scopes only to the shared first-unsigned surface:

- passthrough projection `column(index)` over `uint64`
- `literal<uint64>(value)` projection probes
- `add<uint64>(lhs, rhs)` success, null-propagation, and overflow paths
- `is_not_null(column(index))` over nullable `uint64`
- documented missing-column, mixed signed/unsigned, and unsupported unsigned
  family error carriers
- current normalized TiKV outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in single-engine evidence and adapter behavior as
source evidence, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed:
  `7cb6c3bf9a55ed9f1fc459e16ceed8cb8d39d101`
- artifact baseline: deterministic TiKV adapter-core single-engine plus pairwise
  checkpoint from issue #328
- paired unsigned TiKV drift artifacts now cover `tidb-vs-tikv` and
  `tiflash-vs-tikv`
- no unsigned live-runner refresh artifacts are checked in yet for this slice

## Shared Slice Anchors

This note stays anchored to the stable first-unsigned vocabulary already
defined in `tests/differential/first-unsigned-arithmetic-slice.md`.

- `slice_id = first-unsigned-arithmetic-slice`
- `comparison_mode = row-order-preserved`
- `projection_ref = column-0`
- `projection_ref = column-2`
- `projection_ref = literal-uint64-7`
- `projection_ref = add-uint64-column-0-column-1`
- `filter_ref = is-not-null-column-0`
- `case_id = uint64-column-passthrough`
- `case_id = uint64-literal-projection`
- `case_id = uint64-add-basic`
- `case_id = uint64-add-null-propagation`
- `case_id = uint64-add-overflow-error`
- `case_id = uint64-is-not-null-mixed-keep-drop`
- `case_id = uint64-missing-column-error`
- `case_id = mixed-signed-unsigned-arithmetic-error`
- `case_id = unsupported-unsigned-family-error`

## Reviewed Sources

- `docs/design/first-unsigned-arithmetic-slice.md`
- `adapters/first-unsigned-arithmetic-slice-tikv.md`
- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_unsigned_arithmetic_slice.rs`
- `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice_tikv_pairwise.rs`
- `inventory/first-unsigned-arithmetic-slice-tikv-case-results.json`
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-unsigned-arithmetic-slice-tiflash-vs-tikv-drift-report.md`

## Compatibility Notes

### Projection And Literal Surface

#### TiKV Surface

- the TiKV adapter lowers first-unsigned passthrough and literal projection
  requests into SQL projection over adapter-owned aliases
- checked-in TiKV case-results preserve `uint64` values as canonical base-10
  strings so full-range unsigned values stay exact

#### Recorded TiKV Facts

- `uint64-column-passthrough` returns 3 rows (`"0"`, `"7"`, `"42"`) with
  schema `u:uint64, nullable=false`
- `uint64-literal-projection` returns 3 rows (`"7"`, `"7"`, `"7"`) with
  schema `seven:uint64, nullable=false`

### `add<uint64>` Success And Null Propagation

#### TiKV Surface

- the TiKV adapter lowers first-unsigned add probes into row-wise SQL
  expression evaluation without widening into signed arithmetic
- nullable `uint64` operands remain nullable in the normalized carrier

#### Recorded TiKV Facts

- `uint64-add-basic` returns 3 rows (`"3"`, `"7"`, `"30"`) with schema
  `sum:uint64, nullable=false`
- `uint64-add-null-propagation` returns 3 rows (`null`, `null`, `"7"`) with
  schema `sum:uint64, nullable=true`

### `is_not_null(column(index))` Unsigned Filtering

#### TiKV Surface

- the TiKV adapter lowers the first-unsigned filter probe into SQL
  `WHERE ... IS NOT NULL`
- kept-row order stays aligned with the shared
  `comparison_mode = row-order-preserved` checkpoint

#### Recorded TiKV Facts

- `uint64-is-not-null-mixed-keep-drop` retains 2 rows (`"5"`, `"9"`) with
  schema `u:uint64, nullable=true`

### Unsigned Overflow Error Normalization

#### TiKV Surface

- the first unsigned arithmetic boundary treats `add<uint64>` overflow as an
  execution error rather than a wrap or saturation path
- TiKV normalization maps overflow signals into
  `error_class = unsigned_overflow`

#### Recorded TiKV Facts

- `uint64-add-overflow-error` normalizes to
  `error_class = unsigned_overflow`
- current checked-in TiKV artifact reports `engine_code = 1690` with message
  `BIGINT UNSIGNED value is out of range in '(input_rows.lhs + input_rows.rhs)'`

### Missing Column Error Normalization

#### TiKV Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_2`) for the documented out-of-range projection case
- error normalization maps TiKV missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiKV Facts

- `uint64-missing-column-error` normalizes to
  `error_class = missing_column`
- current checked-in TiKV artifact reports `engine_code = 1054` with message
  `Unknown column '__missing_column_2' in 'field list'`

### Mixed Signed/Unsigned Error Normalization

#### TiKV Surface

- the first unsigned arithmetic slice keeps mixed `int64`/`uint64` arithmetic
  out of scope for successful execution
- TiKV normalization records this checkpoint as shared
  `error_class = mixed_signed_unsigned`

#### Recorded TiKV Facts

- `mixed-signed-unsigned-arithmetic-error` normalizes to
  `error_class = mixed_signed_unsigned`
- current checked-in TiKV artifact reports `engine_code = 1105` with an
  adapter-owned message indicating `Int64` input is unsupported for the shared
  first-unsigned checkpoint

### Unsupported Unsigned Family Normalization

#### TiKV Surface

- the first unsigned boundary admits only `uint64`; `uint32` remains a
  documented unsupported path
- TiKV normalization maps this case into shared
  `error_class = unsupported_unsigned_family`

#### Recorded TiKV Facts

- `unsupported-unsigned-family-error` normalizes to
  `error_class = unsupported_unsigned_family`
- current checked-in TiKV artifact reports `engine_code = 1105` with an
  adapter-owned message indicating `uint32` input is out of scope for this
  slice

## Differential Summary Link

- paired TiKV unsigned drift reports record `match = 9`, `drift = 0`, and
  `unsupported = 0` across both `tidb-vs-tikv` and `tiflash-vs-tikv`

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the first
  unsigned arithmetic slice
- it does not redefine the shared adapter request or response contract
- it does not add live-runner or production environment captures
- checked-in evidence for this checkpoint includes the TiKV single-engine
  `case-results` artifact plus paired TiDB-vs-TiKV and TiFlash-vs-TiKV
  `drift-report` artifacts
- broader unsigned families, mixed signed/unsigned success semantics, and
  unsigned live-runner refresh remain follow-on work
