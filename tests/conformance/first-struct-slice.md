# First Struct Slice Cases

Status: issue #226 docs checkpoint, issue #329 local executable kernel checkpoint

Spec source: `docs/design/first-struct-aware-handoff-slice.md`

## Canonical Cases

- `struct column passthrough`: `column(index)` over
  `struct<a:int32, b:int32?>` preserves row count, field order, top-level null
  placement, child values, child nullability, and field nullability
- `struct nullable passthrough`: nullable `struct` input preserves SQL `NULL`
  row placement while preserving child values for non-null rows
- `struct child-null preservation`: nullable child field values remain null only
  in the affected child field and do not mark the full struct row null
- `missing column`: out-of-range `column(index)` in projection paths fails as
  an execution error
- `unsupported nested family`: nested inputs outside this checkpoint (for
  example `map` or `union`) fail as execution errors in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-struct-slice.md`
- `adapters/first-struct-slice.md`

## Executable Harness Boundary

Issue #329 adds the first executable local kernel conformance coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/struct_slice.rs`
