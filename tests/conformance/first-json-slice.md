# First JSON Slice Cases

Status: issue #224 docs checkpoint, issue #354 executable local conformance checkpoint

Spec source: `docs/design/first-json-semantic-slice.md`

## Canonical Cases

- `json column passthrough`: `column(index)` over `json` preserves row count,
  canonical JSON value tokens, SQL `NULL` placement, and field nullability
- `json nullable passthrough`: nullable `json` input preserves SQL `NULL` rows
  while keeping JSON literal `null` as a non-null JSON value token
- `json object canonicalization`: object values with different key order
  normalize to the same canonical JSON value token for shared comparison
- `json array order preserved`: array element order remains significant in JSON
  value tokens (`[1,2]` and `[2,1]` remain distinct)
- `json predicate all kept`: `is_not_null(column(0))` over non-null `json`
  keeps every row, including rows whose JSON value is literal `null`
- `json predicate all dropped`: `is_not_null(column(0))` over all-SQL-`NULL`
  `json` drops every row
- `json predicate mixed keep/drop`: mixed SQL-`NULL` JSON input keeps only
  non-SQL-`NULL` rows while preserving retained-row order and full-row
  passthrough
- `missing column`: out-of-range `column(index)` in projection or predicate
  paths fails as an execution error
- `unsupported json ordering comparison`: JSON ordered-comparison requests in
  this slice fail as execution errors
- `unsupported json cast`: explicit non-identity casts involving `json` fail as
  execution errors in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-json-slice.md`
- `adapters/first-json-slice.md`

## Executable Harness Boundary

This checkpoint now has local executable shared-kernel conformance coverage in `crates/tiforth-kernel/tests/json_slice.rs` for passthrough `column(index)`, `is_not_null(column(index))`, and missing-column error cases. Unsupported JSON ordering-comparison and explicit non-identity cast probes are now exercised in executable TiDB-versus-TiFlash adapter and harness coverage through `crates/tiforth-adapter-tidb/src/first_json_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_json_slice.rs`, and `crates/tiforth-harness-differential/src/first_json_slice.rs`, with checked-in artifacts under `inventory/first-json-slice-*`.
