# First Collation-Sensitive String Slice Cases

Status: issue #233 docs checkpoint, issue #352 executable local checkpoint, issue #358 executable differential checkpoint

Spec source: `docs/design/first-collation-string-slice.md`

## Canonical Cases

- `utf8 column passthrough binary`: `column(index)` over `utf8` preserves row
  count, row values, SQL `NULL` placement, and field nullability under
  `collation_ref = binary`
- `utf8 column passthrough unicode_ci`: `column(index)` over `utf8` preserves
  row count, row values, SQL `NULL` placement, and field nullability under
  `collation_ref = unicode_ci`
- `utf8 predicate all kept`: `is_not_null(column(0))` over non-null `utf8`
  keeps every row
- `utf8 predicate mixed keep/drop`: mixed nullable `utf8` input keeps only
  non-SQL-`NULL` rows while preserving retained-row order
- `utf8 binary equality case-sensitive`: `collation_eq(column(0), "alpha",
  binary)` treats `"Alpha"` and `"alpha"` as unequal values
- `utf8 unicode_ci equality case-insensitive`:
  `collation_eq(column(0), "alpha", unicode_ci)` treats `"Alpha"` and
  `"alpha"` as equal values
- `utf8 binary less-than`: `collation_lt(column(0), "beta", binary)` follows
  bytewise UTF-8 ordering
- `utf8 unicode_ci ordering normalization`:
  `order_by(column(0), unicode_ci, asc)` uses folded keys for ordering intent
  with deterministic tie-breakers for canonical row carriers
- `missing column`: out-of-range `column(index)` in projection or predicate
  paths fails as an execution error
- `unknown collation`: unsupported `collation_ref` fails as an execution error
- `unsupported collation type`: collation-sensitive comparison over non-`utf8`
  input fails as an execution error

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-collation-string-slice.md`
- `adapters/first-collation-string-slice.md`

## Executable Harness Boundary

Issue #352 adds first executable local shared-kernel coverage for this checkpoint in `crates/tiforth-kernel/tests/collation_string_slice.rs`. Issue #358 adds executable TiDB-versus-TiFlash adapter and differential harness coverage for this checkpoint in `crates/tiforth-adapter-tidb/src/first_collation_string_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_collation_string_slice.rs`, and `crates/tiforth-harness-differential/src/first_collation_string_slice.rs`, with checked-in artifacts under `inventory/first-collation-string-slice-*`.
